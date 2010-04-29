using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;

using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	internal class NetworkTreeStorageSystem : ITreeStorageSystem {
		internal NetworkTreeStorageSystem(IServiceConnector connector, ServiceAddress managerAddress, INetworkCache networkCache) {
			this.connector = connector;
			this.managerAddress = managerAddress;
			this.networkCache = networkCache;
			failures = new Dictionary<ServiceAddress, DateTime>();
			pathToRoot = new Dictionary<string, ServiceAddress>();
		}

		private readonly IServiceConnector connector;
		private readonly ServiceAddress managerAddress;
		private readonly INetworkCache networkCache;

		private readonly Dictionary<ServiceAddress, DateTime> failures;
		private readonly Dictionary<string, ServiceAddress> pathToRoot;
		private readonly Dictionary<ServiceAddress, int> proximityMap = new Dictionary<ServiceAddress, int>();

		private const short LeafType = 0x019EC;
		private const short BranchType = 0x022EB;

		private void ReportBlockServerFailure(ServiceAddress address) {
			//TODO: WARN log ...

			// Failure throttling,
			lock (failures) {
				DateTime current_time = DateTime.Now;
				DateTime last_address_fail_time;
				if (failures.TryGetValue(address, out last_address_fail_time) &&
					last_address_fail_time.AddMilliseconds(30 * 1000) > current_time) {
					// We don't respond to failure notifications on the same address if a
					// failure notice arrived within a minute of the last one accepted.
					return;
				}
				failures.Add(address, current_time);
			}

			IMessageProcessor manager = connector.Connect(managerAddress, ServiceType.Manager);
			MessageStream message_out = new MessageStream(16);
			message_out.StartMessage("notifyBlockServerFailure");
			message_out.AddMessageArgument(address);
			message_out.CloseMessage();
			// Process the failure report message on the manager server,
			MessageStream message_in = manager.Process(message_out);
			foreach (Message m in message_in) {
				if (m is ErrorMessage) {
					//TODO: ERROR log ...
				}
			}
		}

		private int GetProximity(ServiceAddress node) {
			lock (proximityMap) {
				int closeness;
				if (!proximityMap.TryGetValue(node, out closeness)) {
					try {
						IPAddress machine_address = node.ToIPAddress();

						NetworkInterface[] local_interfaces = NetworkInterface.GetAllNetworkInterfaces();
						bool is_local = false;
						foreach (NetworkInterface netint in local_interfaces) {
							if (netint.GetIPProperties().DnsAddresses.Contains(machine_address)) {
								is_local = true;
								break;
							}
						}
						// If the interface is local,
						if (is_local) {
							closeness = 0;
						} else {
							// Not local,
							closeness = 10000;
						}

					} catch (SocketException e) {
						// Unknown closeness,
						// Log a severe error,
						//TODO: ERROR log ...
						closeness = Int32.MaxValue;
					}

					// Put it in the map,
					proximityMap.Add(node, closeness);
				}
				return closeness;
			}
		}

		private IDictionary<long, IList<BlockServerElement>> GetServersForBlock(List<long> block_ids) {
			// The result map,
			Dictionary<long, IList<BlockServerElement>> result_map = new Dictionary<long, IList<BlockServerElement>>();

			List<long> none_cached = new List<long>(block_ids.Count);
			foreach (long block_id in block_ids) {
				IList<BlockServerElement> v = networkCache.GetServers(block_id);
				// If it's cached (and the cache is current),
				if (v != null) {
					result_map.Add(block_id, v);
				}
					// If not cached, add to the list of none cached entries,
				else {
					none_cached.Add(block_id);
				}
			}

			// If there are no 'none_cached' blocks,
			if (none_cached.Count == 0) {
				// Return the result,
				return result_map;
			}

			// Otherwise, we query the manager server for current records on the given
			// blocks.

			IMessageProcessor manager = connector.Connect(managerAddress, ServiceType.Manager);
			MessageStream message_out = new MessageStream(15);

			foreach (long block_id in none_cached) {
				message_out.StartMessage("getServerList");
				message_out.AddMessageArgument(block_id);
				message_out.CloseMessage();
			}

			MessageStream message_in = manager.Process(message_out);

			int n = 0;
			foreach (Message m in message_in) {
				if (m is ErrorMessage) {
					ErrorMessage errMsg = (ErrorMessage) m;
					throw new Exception(errMsg.Error.Message, errMsg.Error);
				} else {
					int sz = (int)m[0];
					List<BlockServerElement> srvs = new List<BlockServerElement>(sz);
					for (int i = 0; i < sz; ++i) {
						ServiceAddress address = (ServiceAddress)m[1 + (i * 2)];
						string status = (string)m[1 + (i * 2) + 1];
						srvs.Add(new BlockServerElement(address, status));
					}

					// Shuffle the list
					CollectionsUtil.Shuffle(srvs);

					// Move the server closest to this node to the start of the list,
					int closest = 0;
					int cur_close_factor = Int32.MaxValue;
					for (int i = 0; i < sz; ++i) {
						BlockServerElement elem = srvs[i];
						int closeness_factor = GetProximity(elem.Address);
						if (closeness_factor < cur_close_factor) {
							cur_close_factor = closeness_factor;
							closest = i;
						}
					}

					// Swap if necessary,
					if (closest > 0) {
						CollectionsUtil.Swap(srvs, 0, closest);
					}

					// Put it in the result map,
					long block_id = none_cached[n];
					result_map.Add(block_id, srvs);
					// Add it to the cache,
					// NOTE: TTL hard-coded to 15 minute
					networkCache.SetServers(block_id, srvs, 15 * 60 * 1000);

				}
				++n;
			}

			// Return the list
			return result_map;
		}

		private List<long> DoPersist(TreeWrite sequence, int try_count) {
			// NOTE: nodes are written in order of branches and then leaf nodes. All
			//   branch nodes and leafs are grouped together.

			// The list of nodes to be allocated,
			IList<ITreeNode> all_branches = sequence.BranchNodes;
			IList<ITreeNode> all_leafs = sequence.LeafNodes;
			List<ITreeNode> nodes = new List<ITreeNode>(all_branches.Count + all_leafs.Count);
			nodes.AddRange(all_branches);
			nodes.AddRange(all_leafs);
			int sz = nodes.Count;
			// The list of allocated referenced for the nodes,
			DataAddress[] refs = new DataAddress[sz];
			List<long> out_refs = new List<long>(sz);

			MessageStream allocate_message = new MessageStream((sz * 3) + 16);

			// Make a connection with the manager server,
			IMessageProcessor manager = connector.Connect(managerAddress, ServiceType.Manager);

			// Allocate the space first,
			for (int i = 0; i < sz; ++i) {
				ITreeNode node = nodes[i];
				// Is it a branch node?
				if (node is TreeBranch) {
					// Branch nodes are 1K in size,
					allocate_message.StartMessage("allocateNode");
					allocate_message.AddMessageArgument(1024);
					allocate_message.CloseMessage();
				}
					// Otherwise, it must be a leaf node,
				else {
					// Leaf nodes are 4k in size,
					allocate_message.StartMessage("allocateNode");
					allocate_message.AddMessageArgument(4096);
					allocate_message.CloseMessage();
				}
			}

			// The result of the set of allocations,
			MessageStream result_stream = manager.Process(allocate_message);
			//DEBUG: ++network_comm_count;

			// The unique list of blocks,
			List<long> unique_blocks = new List<long>();

			// Parse the result stream one message at a time, the order will be the
			// order of the allocation messages,
			int n = 0;
			foreach (Message m in result_stream) {
				if (m is ErrorMessage)
					throw ((ErrorMessage) m).Error;

				DataAddress addr = (DataAddress) m[0];
				refs[n] = addr;
				// Make a list of unique block identifiers,
				if (!unique_blocks.Contains(addr.BlockId)) {
					unique_blocks.Add(addr.BlockId);
				}
				++n;
			}

			// Get the block to server map for each of the blocks,

			IDictionary<long, IList<BlockServerElement>> block_to_server_map = GetServersForBlock(unique_blocks);

			// Make message streams for each unique block
			int ubid_count = unique_blocks.Count;
			MessageStream[] ubid_stream = new MessageStream[ubid_count];
			for (int i = 0; i < ubid_stream.Length; ++i) {
				ubid_stream[i] = new MessageStream(512);
			}

			// Scan all the blocks and create the message streams,
			for (int i = 0; i < sz; ++i) {
				byte[] node_buf;

				ITreeNode node = nodes[i];
				// Is it a branch node?
				if (node is TreeBranch) {
					TreeBranch branch = (TreeBranch)node;
					// Make a copy of the branch (NOTE; we clone() the array here).
					long[] cur_node_data = (long[])branch.ChildPointers.Clone();
					int cur_ndsz = branch.DataSize;
					branch = new TreeBranch(refs[i].Value, cur_node_data, cur_ndsz);

					// The number of children
					int chsz = branch.ChildCount;
					// For each child, if it's a heap node, look up the child id and
					// reference map in the sequence and set the reference accordingly,
					for (int o = 0; o < chsz; ++o) {
						long child_ref = branch.GetChild(o);
						if (child_ref < 0) {
							// The ref is currently on the heap, so adjust accordingly
							int ref_id = sequence.LookupRef(i, o);
							branch.SetChildOverride(o, refs[ref_id].Value);
						}
					}

					// Turn the branch into a 'node_buf' byte[] array object for
					// serialization.
					long[] node_data = branch.ChildPointers;
					int ndsz = branch.DataSize;
					MemoryStream bout = new MemoryStream(1024);
					BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
					dout.Write(BranchType);
					dout.Write(ndsz);
					for (int o = 0; o < ndsz; ++o) {
						dout.Write(node_data[o]);
					}
					dout.Flush();

					// Turn it into a byte array,
					node_buf = bout.ToArray();

					// Put this branch into the local cache,
					networkCache.SetNode(refs[i], branch);

				}
					// If it's a leaf node,
				else {

					TreeLeaf leaf = (TreeLeaf)node;
					int lfsz = leaf.Length;

					node_buf = new byte[lfsz + 6];
					// Technically, we could comment these next two lines out.
					Util.ByteBuffer.WriteInt2(LeafType, node_buf, 0);
					Util.ByteBuffer.WriteInteger(lfsz, node_buf, 2);
					leaf.Read(0, node_buf, 6, lfsz);

					// Put this leaf into the local cache,
					leaf = new ByteArrayTreeLeaf(refs[i].Value, node_buf);
					networkCache.SetNode(refs[i], leaf);

				}

				// The DataAddress this node is being written to,
				DataAddress address = refs[i];
				// Get the block id,
				long block_id = address.BlockId;
				int bid = unique_blocks.IndexOf(block_id);
				ubid_stream[bid].StartMessage("writeToBlock");
				ubid_stream[bid].AddMessageArgument(address);
				ubid_stream[bid].AddMessageArgument(node_buf);
				ubid_stream[bid].AddMessageArgument(0);
				ubid_stream[bid].AddMessageArgument(node_buf.Length);
				ubid_stream[bid].CloseMessage();

				// Update 'out_refs' array,
				out_refs[i] = refs[i].Value;

			}

			// A log of successfully processed operations,
			List<object> success_process = new List<object>(64);

			// Now process the streams on the servers,
			for (int i = 0; i < ubid_stream.Length; ++i) {
				// The output message,
				MessageStream message_out = ubid_stream[i];
				// Get the servers this message needs to be sent to,
				long block_id = unique_blocks[i];
				IList<BlockServerElement> block_servers = block_to_server_map[block_id];
				// Format a message for writing this node out,
				int bssz = block_servers.Count;
				IMessageProcessor[] block_server_procs = new IMessageProcessor[bssz];
				// Make the block server connections,
				for (int o = 0; o < bssz; ++o) {
					ServiceAddress address = block_servers[o].Address;
					block_server_procs[o] = connector.Connect(address, ServiceType.Block);
					MessageStream message_in = block_server_procs[o].Process(message_out);
					//DEBUG: ++network_comm_count;

					foreach (Message m in message_in) {
						if (m is ErrorMessage) {
							// If this is an error, we need to report the failure to the
							// manager server,
							ReportBlockServerFailure(address);
							// Remove the block id from the server list cache,
							networkCache.RemoveServers(block_id);

							// Rollback any server writes already successfully made,
							for (int p = 0; p < success_process.Count; p += 2) {
								ServiceAddress blocks_addr = (ServiceAddress)success_process[p];
								MessageStream to_rollback = (MessageStream)success_process[p + 1];

								List<DataAddress> rollback_nodes = new List<DataAddress>(128);
								foreach (Message rm in to_rollback) {
									DataAddress raddr = (DataAddress)rm[0];
									rollback_nodes.Add(raddr);
								}
								// Create the rollback message,
								MessageStream rollback_msg = new MessageStream(16);
								rollback_msg.StartMessage("rollbackNodes");
								rollback_msg.AddMessageArgument(rollback_nodes.ToArray());
								rollback_msg.CloseMessage();

								// Send it to the block server,
								MessageStream msg_in = connector.Connect(blocks_addr, ServiceType.Block).Process(rollback_msg);
								//DEBUG: ++network_comm_count;
								foreach (Message rbm in msg_in) {
									// If rollback generated an error we throw the error now
									// because this likely is a serious network error.
									if (rbm is ErrorMessage) {
										throw new NetworkException("Rollback wrote failed: " + ((ErrorMessage) rbm).Error.Message);
									}
								}

							}

							// Retry,
							if (try_count > 0)
								return DoPersist(sequence, try_count - 1);
								
							// Otherwise we fail the write
							throw new NetworkException(((ErrorMessage) m).Error.Message);
						}
					}

					// If we succeeded without an error, add to the log
					success_process.Add(address);
					success_process.Add(message_out);

				}
			}

			// Return the references,
			return out_refs;

		}

		public DataAddress CreateEmptyDatabase() {
			TreeNodeHeap node_heap = new TreeNodeHeap(17, 4 * 1024 * 1024);

			TreeLeaf head_leaf = node_heap.CreateLeaf(null, Key.Head, 256);
			// Insert a tree identification pattern
			head_leaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
			// Create an empty tail node
			TreeLeaf tail_leaf = node_heap.CreateLeaf(null, Key.Tail, 256);
			// Insert a tree identification pattern
			tail_leaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

			// The write sequence,
			TreeWrite seq = new TreeWrite();
			seq.NodeWrite(head_leaf);
			seq.NodeWrite(tail_leaf);
			IList<long> refs = Persist(seq);

			// Create a branch,
			int maxBranchSize = GetConfigValue<int>("maxBranchSize");
			TreeBranch root_branch = node_heap.CreateBranch(null, maxBranchSize);
			root_branch.Set(refs[0], 4,
			                Key.Tail.GetEncoded((1)), 
							Key.Tail.GetEncoded((2)),
			                refs[1], 4);

			seq = new TreeWrite();
			seq.NodeWrite(root_branch);
			refs = Persist(seq);

			// The written root node reference,
			long root_id = refs[0];

			// Delete the head and tail leaf, and the root branch
			node_heap.Delete(head_leaf.Id);
			node_heap.Delete(tail_leaf.Id);
			node_heap.Delete(root_branch.Id);

			// Return the root,
			return new DataAddress(root_id);
		}

		#region Implementation of ITreeStorageSystem

		public T GetConfigValue<T>(string key) {
			throw new NotImplementedException();
		}

		public void CheckPoint() {
			throw new NotImplementedException();
		}

		public IList<T> FetchNodes<T>(long[] nids) where T : ITreeNode {
			throw new NotImplementedException();
		}

		public void DisposeNode(long nid) {
			throw new NotImplementedException();
		}

		public Exception SetErrorState(Exception error) {
			throw new NotImplementedException();
		}

		public void CheckErrorState() {
			throw new NotImplementedException();
		}

		public IList<long> Persist(TreeWrite write) {
			return DoPersist(write, 1);
		}

		#endregion

		#region MemoryTreeLeaf

		private sealed class ByteArrayTreeLeaf : TreeLeaf {

			private readonly byte[] buffer;
			private readonly long id;

			public ByteArrayTreeLeaf(long id, byte[] buffer) {
				this.id = id;
				this.buffer = buffer;
			}

			// ---------- Implemented from TreeLeaf ----------

			public override long Id {
				get { return id; }
			}

			public override int Length {
				get { return buffer.Length - 6; }
			}

			public override int Capacity {
				get { throw new Exception(); }
			}

			public override void Read(int position, byte[] buf, int off, int len) {
				Array.Copy(buffer, 6 + position, buf, off, len);
			}

			public override void WriteTo(IAreaWriter writer) {
				writer.Write(buffer, 6, Length);
			}

			public override void Shift(int position, int offset) {
				throw new IOException("Cannot write an immutable leaf");
			}

			public override void Write(int position, byte[] buf, int off, int len) {
				throw new IOException("Cannot write an immutable leaf");
			}

			public override void SetLength(int size) {
				throw new IOException("Cannot write an immutable leaf");
			}

			public override long MemoryAmount {
				get { return 8 + buffer.Length + 64; }
			}

		}

		#endregion
	}
}