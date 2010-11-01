using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;
using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	internal class NetworkTreeSystem : ITreeSystem {
		internal NetworkTreeSystem(IServiceConnector connector, IServiceAddress managerAddress, INetworkCache networkCache) {
			if (!(connector.MessageSerializer is IMessageStreamSupport))
				throw new ArgumentException("The message serializer specified by the connector is not valid (must be RPC).");
			
			this.connector = connector;
			this.managerAddress = managerAddress;
			this.networkCache = networkCache;
			failures = new Dictionary<IServiceAddress, DateTime>();
			pathToRoot = new Dictionary<string, IServiceAddress>();
			
			logger = LogManager.NetworkLogger;
		}

		private readonly IServiceConnector connector;
		private readonly IServiceAddress managerAddress;
		private readonly INetworkCache networkCache;

		private readonly Dictionary<IServiceAddress, DateTime> failures;
		private readonly Dictionary<string, IServiceAddress> pathToRoot;
		private readonly Dictionary<IServiceAddress, int> proximityMap = new Dictionary<IServiceAddress, int>();

		private readonly Object reachability_lock = new Object();
		private int reachability_tree_depth;

		private long max_transaction_node_heap_size;

		private ErrorStateException errorState;
		private Logger logger;

		private const short LeafType = 0x019EC;
		private const short BranchType = 0x022EB;

		private void ReportBlockServerFailure(IServiceAddress address) {
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
			RequestMessage requestMessage = new RequestMessage("notifyBlockServerFailure");
			requestMessage.Arguments.Add(address);
			// Process the failure report message on the manager server,
			ResponseMessage responseMessage = manager.Process(requestMessage);
			if (responseMessage.HasError)
				logger.Error("Error found while processing 'notifyBlockServerFailure': " + responseMessage.ErrorMessage);
		}

		//TODO: should this work also for other kind of addresses?
		private int GetProximity(IServiceAddress node) {			
			lock (proximityMap) {
				int closeness;
				if (!proximityMap.TryGetValue(node, out closeness)) {
					try {
						if (!(node is TcpServiceAddress))
							throw new NotSupportedException("This algorithm is not supported for this kind of service.");

						IPAddress machine_address = ((TcpServiceAddress) node).ToIPAddress();

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

					} catch (Exception e) {
						// Unknown closeness,
						// Log a severe error,
						logger.Error("Cannot determine the proximity factor for address " + node);
						closeness = Int32.MaxValue;
					}

					// Put it in the map,
					proximityMap.Add(node, closeness);
				}
				return closeness;
			}
		}

		private IDictionary<long, IList<BlockServerElement>> GetServersForBlock(IList<long> blockIds) {
			// The result map,
			Dictionary<long, IList<BlockServerElement>> resultMap = new Dictionary<long, IList<BlockServerElement>>();

			List<long> noneCached = new List<long>(blockIds.Count);
			foreach (long blockId in blockIds) {
				IList<BlockServerElement> v = networkCache.GetServers(blockId);
				// If it's cached (and the cache is current),
				if (v != null) {
					resultMap.Add(blockId, v);
				}
					// If not cached, add to the list of none cached entries,
				else {
					noneCached.Add(blockId);
				}
			}

			// If there are no 'none_cached' blocks,
			if (noneCached.Count == 0)
				// Return the result,
				return resultMap;

			// Otherwise, we query the manager server for current records on the given
			// blocks.

			IMessageProcessor manager = connector.Connect(managerAddress, ServiceType.Manager);
			RequestMessageStream message_out = new RequestMessageStream();

			foreach (long block_id in noneCached) {
				RequestMessage request = new RequestMessage("getServerListForBlock");
				request.Arguments.Add(block_id);
				message_out.AddMessage(request);
			}

			ResponseMessageStream message_in = (ResponseMessageStream) manager.Process(message_out);

			int n = 0;
			foreach (ResponseMessage m in message_in) {
				if (m.HasError)
					throw new Exception(m.ErrorMessage, m.Error.AsException());

				int sz = m.Arguments[0].ToInt32();
				List<BlockServerElement> srvs = new List<BlockServerElement>(sz);
				IServiceAddress[] addresses = (IServiceAddress[]) m.Arguments[1].Value;
				int[] status = (int[]) m.Arguments[2].Value;
				for (int i = 0; i < sz; ++i) {
					srvs.Add(new BlockServerElement(addresses[i], (ServiceStatus) status[i]));
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
				long block_id = noneCached[n];
				resultMap.Add(block_id, srvs);
				// Add it to the cache,
				// NOTE: TTL hard-coded to 15 minute
				networkCache.SetServers(block_id, srvs, 15*60*1000);

				++n;
			}

			// Return the list
			return resultMap;
		}

		private static bool IsInNodeList(long reference, IList<ITreeNode> nodes) {
			foreach (ITreeNode node in nodes) {
				if (reference == node.Id)
					return true;
			}
			return false;
		}

		private static byte[] ReadNodeAsBuffer(Node node) {
			Stream input = node.Input;
			MemoryStream outputStream = new MemoryStream();
			int readCount;
			byte[] buffer = new byte[1024];
			while ((readCount = input.Read(buffer, 0, 1024)) != 0) {
				outputStream.Write(buffer, 0, readCount);
			}

			return outputStream.ToArray();
		}

		private TreeGraph CreateDiagnosticRootGraph(Key left_key, long reference) {

			// The node being returned
			TreeGraph node;

			// Fetch the node,
			ITreeNode tree_node = FetchNodes(new long[] { reference })[0];

			if (tree_node is TreeLeaf) {
				TreeLeaf leaf = (TreeLeaf)tree_node;
				// The number of bytes in the leaf
				int leaf_size = leaf.Length;

				// Set up the leaf node object
				node = new TreeGraph("leaf", reference);
				node.SetProperty("key", left_key.ToString());
				node.SetProperty("leaf_size", leaf_size);

			} else if (tree_node is TreeBranch) {
				TreeBranch branch = (TreeBranch)tree_node;
				// Set up the branch node object
				node = new TreeGraph("branch", reference);
				node.SetProperty("key", left_key.ToString());
				node.SetProperty("branch_size", branch.ChildCount);
				// Recursively add each child into the tree
				for (int i = 0; i < branch.ChildCount; ++i) {
					long child_ref = branch.GetChild(i);
					// If the ref is a special node, skip it
					if ((child_ref & 0x01000000000000000L) != 0) {
						// Should we record special nodes?
					} else {
						Key new_left_key = (i > 0) ? branch.GetKey(i) : left_key;
						TreeGraph bn = new TreeGraph("child_meta", reference);
						bn.SetProperty("extent", branch.GetChildLeafElementCount(i));
						node.AddChild(bn);
						node.AddChild(CreateDiagnosticRootGraph(new_left_key, child_ref));
					}
				}
			} else {
				throw new IOException("Unknown node class: " + tree_node);
			}

			return node;
		}

		private void DoReachCheck(TextWriter warning_log, long node, IIndex node_list, int cur_depth) {
			// Is the node in the list?
			bool inserted = node_list.InsertUnique(node, node, SortedIndex.KeyComparer);

			if (inserted) {
				// Fetch the node,
				try {
					ITreeNode tree_node = FetchNodes(new long[] { node })[0];
					if (tree_node is TreeBranch) {
						// Get the child nodes,
						TreeBranch branch = (TreeBranch)tree_node;
						int children_count = branch.ChildCount;
						for (int i = 0; i < children_count; ++i) {
							long child_node_ref = branch.GetChild(i);
							// Recurse,
							if (cur_depth + 1 == reachability_tree_depth) {
								// It's a known leaf node, so insert now without traversing
								node_list.InsertUnique(child_node_ref, child_node_ref, SortedIndex.KeyComparer);
							} else {
								// Recurse,
								DoReachCheck(warning_log, child_node_ref, node_list, cur_depth + 1);
							}
						}
					} else if (tree_node is TreeLeaf) {
						reachability_tree_depth = cur_depth;
					} else {
						throw new IOException("Unknown node class: " + tree_node);
					}
				} catch (InvalidDataState e) {
					// Report the error,
					warning_log.WriteLine("Invalid Data Set (msg: " + e.Message + ")");
					warning_log.WriteLine("Block: " + e.Address.BlockId);
					warning_log.WriteLine("Data:  " + e.Address.DataId);
				}
			}
		}

		private List<long> DoPersist(TreeWrite sequence, int tryCount) {
			// NOTE: nodes are written in order of branches and then leaf nodes. All
			//   branch nodes and leafs are grouped together.

			// The list of nodes to be allocated,
			IList<ITreeNode> allBranches = sequence.BranchNodes;
			IList<ITreeNode> allLeafs = sequence.LeafNodes;
			List<ITreeNode> nodes = new List<ITreeNode>(allBranches.Count + allLeafs.Count);
			nodes.AddRange(allBranches);
			nodes.AddRange(allLeafs);
			int sz = nodes.Count;
			// The list of allocated referenced for the nodes,
			DataAddress[] refs = new DataAddress[sz];
			long[] outRefs = new long[sz];

			RequestMessageStream allocateMessage = new RequestMessageStream();

			// Make a connection with the manager server,
			IMessageProcessor manager = connector.Connect(managerAddress, ServiceType.Manager);

			// Allocate the space first,
			for (int i = 0; i < sz; ++i) {
				ITreeNode node = nodes[i];
				RequestMessage request = new RequestMessage("allocateNode");
				// Is it a branch node?
				if (node is TreeBranch) {
					// Branch nodes are 1K in size,
					request.Arguments.Add(1024);
				} else {
					// Leaf nodes are 4k in size,
					request.Arguments.Add(4096);
				}

				allocateMessage.AddMessage(request);
			}

			// The result of the set of allocations,
			ResponseMessageStream resultStream = (ResponseMessageStream) manager.Process(allocateMessage);
			//DEBUG: ++network_comm_count;

			// The unique list of blocks,
			List<long> uniqueBlocks = new List<long>();

			// Parse the result stream one message at a time, the order will be the
			// order of the allocation messages,
			int n = 0;
			foreach (ResponseMessage m in resultStream) {
				if (m.HasError)
					throw m.Error.AsException();

				DataAddress addr = (DataAddress) m.Arguments[0].Value;
				refs[n] = addr;
				// Make a list of unique block identifiers,
				if (!uniqueBlocks.Contains(addr.BlockId)) {
					uniqueBlocks.Add(addr.BlockId);
				}
				++n;
			}

			// Get the block to server map for each of the blocks,

			IDictionary<long, IList<BlockServerElement>> blockToServerMap = GetServersForBlock(uniqueBlocks);

			// Make message streams for each unique block
			int ubid_count = uniqueBlocks.Count;
			RequestMessageStream[] ubidStream = new RequestMessageStream[ubid_count];
			for (int i = 0; i < ubidStream.Length; ++i) {
				ubidStream[i] = new RequestMessageStream();
			}

			// Scan all the blocks and create the message streams,
			for (int i = 0; i < sz; ++i) {
				byte[] nodeBuf;

				ITreeNode node = nodes[i];
				// Is it a branch node?
				if (node is TreeBranch) {
					TreeBranch branch = (TreeBranch)node;
					// Make a copy of the branch (NOTE; we Clone() the array here).
					long[] curNodeData = (long[])branch.ChildPointers.Clone();
					int curNdsz = branch.DataSize;
					branch = new TreeBranch(refs[i].Value, curNodeData, curNdsz);

					// The number of children
					int chsz = branch.ChildCount;
					// For each child, if it's a heap node, look up the child id and
					// reference map in the sequence and set the reference accordingly,
					for (int o = 0; o < chsz; ++o) {
						long childRef = branch.GetChild(o);
						if (childRef < 0) {
							// The ref is currently on the heap, so adjust accordingly
							int ref_id = sequence.LookupRef(i, o);
							branch.SetChildOverride(o, refs[ref_id].Value);
						}
					}

					// Turn the branch into a 'node_buf' byte[] array object for
					// serialization.
					long[] nodeData = branch.ChildPointers;
					int ndsz = branch.DataSize;
					MemoryStream bout = new MemoryStream(1024);
					BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
					dout.Write(BranchType);
					dout.Write(ndsz);
					for (int o = 0; o < ndsz; ++o) {
						dout.Write(nodeData[o]);
					}
					dout.Flush();

					// Turn it into a byte array,
					nodeBuf = bout.ToArray();

					// Put this branch into the local cache,
					networkCache.SetNode(refs[i], branch);

				} else {
					// If it's a leaf node,

					TreeLeaf leaf = (TreeLeaf)node;
					int lfsz = leaf.Length;

					nodeBuf = new byte[lfsz + 6];
					// Technically, we could comment these next two lines out.
					ByteBuffer.WriteInt2(LeafType, nodeBuf, 0);
					ByteBuffer.WriteInt4(lfsz, nodeBuf, 2);
					leaf.Read(0, nodeBuf, 6, lfsz);

					// Put this leaf into the local cache,
					leaf = new ByteArrayTreeLeaf(refs[i].Value, nodeBuf);
					networkCache.SetNode(refs[i], leaf);

				}

				// The DataAddress this node is being written to,
				DataAddress address = refs[i];
				// Get the block id,
				long blockId = address.BlockId;
				int bid = uniqueBlocks.IndexOf(blockId);
				RequestMessage request = new RequestMessage("writeToBlock");
				request.Arguments.Add(address);
				request.Arguments.Add(nodeBuf);
				request.Arguments.Add(0);
				request.Arguments.Add(nodeBuf.Length);
				ubidStream[bid].AddMessage(request);

				// Update 'outRefs' array,
				outRefs[i] = refs[i].Value;
			}

			// A log of successfully processed operations,
			List<object> successProcess = new List<object>(64);

			// Now process the streams on the servers,
			for (int i = 0; i < ubidStream.Length; ++i) {
				// The output message,
				RequestMessageStream requestMessageStream = ubidStream[i];

				// Get the servers this message needs to be sent to,
				long block_id = uniqueBlocks[i];
				IList<BlockServerElement> blockServers = blockToServerMap[block_id];

				// Format a message for writing this node out,
				int bssz = blockServers.Count;
				IMessageProcessor[] blockServerProcs = new IMessageProcessor[bssz];

				// Make the block server connections,
				for (int o = 0; o < bssz; ++o) {
					IServiceAddress address = blockServers[o].Address;
					blockServerProcs[o] = connector.Connect(address, ServiceType.Block);
					ResponseMessageStream responseMessageStream = (ResponseMessageStream) blockServerProcs[o].Process(requestMessageStream);
					//DEBUG: ++network_comm_count;

					if (responseMessageStream.HasError) {
						// If this is an error, we need to report the failure to the
						// manager server,
						ReportBlockServerFailure(address);
						// Remove the block id from the server list cache,
						networkCache.RemoveServers(block_id);

						// Rollback any server writes already successfully made,
						for (int p = 0; p < successProcess.Count; p += 2) {
							IServiceAddress blockAddress = (IServiceAddress) successProcess[p];
							RequestMessageStream toRollback = (RequestMessageStream) successProcess[p + 1];

							List<DataAddress> rollbackNodes = new List<DataAddress>(128);
							foreach(Message rm in toRollback) {
								DataAddress raddr = (DataAddress) rm.Arguments[0].Value;
								rollbackNodes.Add(raddr);
							}

							// Create the rollback message,
							RequestMessage rollbackRequest = new RequestMessage("rollbackNodes");
							rollbackRequest.Arguments.Add(rollbackNodes.ToArray());

							// Send it to the block server,
							ResponseMessage responseMessage = connector.Connect(blockAddress, ServiceType.Block).Process(rollbackRequest);
							//DEBUG: ++network_comm_count;

							// If rollback generated an error we throw the error now
							// because this likely is a serious network error.
							if (responseMessage.HasError)
								throw new NetworkException("Rollback wrote failed: " + responseMessage.ErrorMessage);
						}

						// Retry,
						if (tryCount > 0)
							return DoPersist(sequence, tryCount - 1);

						// Otherwise we fail the write
						throw new NetworkException(responseMessageStream.ErrorMessage);
					}

					// If we succeeded without an error, add to the log
					successProcess.Add(address);
					successProcess.Add(requestMessageStream);

				}
			}

			// Return the references,
			return new List<long>(outRefs);

		}

		internal void SetMaxNodeCacheHeapSize(long value) {
			max_transaction_node_heap_size = value;
		}

		public DataAddress CreateEmptyDatabase() {
			TreeNodeHeap nodeHeap = new TreeNodeHeap(17, 4 * 1024 * 1024);

			TreeLeaf headLeaf = nodeHeap.CreateLeaf(null, Key.Head, 256);
			// Insert a tree identification pattern
			headLeaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

			// Create an empty tail node
			TreeLeaf tailLeaf = nodeHeap.CreateLeaf(null, Key.Tail, 256);
			// Insert a tree identification pattern
			tailLeaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

			// The write sequence,
			TreeWrite seq = new TreeWrite();
			seq.NodeWrite(headLeaf);
			seq.NodeWrite(tailLeaf);
			IList<long> refs = Persist(seq);

			// Create a branch,
			int maxBranchSize = MaxBranchSize;
			TreeBranch rootBranch = nodeHeap.CreateBranch(null, maxBranchSize);
			rootBranch.Set(refs[0], 4,
			                Key.Tail.GetEncoded((1)), 
							Key.Tail.GetEncoded((2)),
			                refs[1], 4);

			seq = new TreeWrite();
			seq.NodeWrite(rootBranch);
			refs = Persist(seq);

			// The written root node reference,
			long root_id = refs[0];

			// Delete the head and tail leaf, and the root branch
			nodeHeap.Delete(headLeaf.Id);
			nodeHeap.Delete(tailLeaf.Id);
			nodeHeap.Delete(rootBranch.Id);

			// Return the root,
			return new DataAddress(root_id);
		}

		public IServiceAddress GetRootServer(string pathName) {

			// Check if this is stored in the cache first,
			lock(pathToRoot) {
				IServiceAddress saddr;
				if (pathToRoot.TryGetValue(pathName, out saddr))
					return saddr;
			}

			// It isn't, so query the manager server on the network
			IMessageProcessor manager = connector.Connect(managerAddress, ServiceType.Manager);
			RequestMessage requestMessage = new RequestMessage("getRootForPath");
			requestMessage.Arguments.Add(pathName);
			// Process the 'getRootFor' command,
			ResponseMessage msg_in = manager.Process(requestMessage);
			//DEBUG ++networkCommCount;

			if (msg_in.HasError)
				throw new Exception(msg_in.ErrorMessage, msg_in.Error.AsException());

			// Return the service address result,
			IServiceAddress address = (IServiceAddress) msg_in.Arguments[0].Value;
			// Put it in the map,
			lock(pathToRoot) {
				pathToRoot[pathName] = address;
			}

			return address;
		}

		public ITransaction CreateTransaction(DataAddress rootNode) {
			return new Transaction(this, 0, rootNode);
		}

		public ITransaction CreateEmptyTransaction() {
			return new Transaction(this, 0);
		}

		public DataAddress FlushTransaction(ITransaction transaction) {
			Transaction net_transaction = (Transaction)transaction;

			try {
				net_transaction.CheckOut();
				return new DataAddress(net_transaction.RootNodeId);
			} catch (IOException e) {
				throw new Exception(e.Message, e);
			}
		}

		public void DisposeTransaction(ITransaction transaction) {
			((Transaction)transaction).Dispose();
		}

		public DataAddress Commit(IServiceAddress root_server, String path_name, DataAddress proposal) {
			IMessageProcessor processor = connector.Connect(root_server, ServiceType.Root);
			RequestMessage request = new RequestMessage("commit");
			request.Arguments.Add(path_name);
			request.Arguments.Add(proposal);
			ResponseMessage response = processor.Process(request);

			if (response.HasError) {
				MessageError et = response.Error;
				// If it's a commit fault exception, wrap it locally.
				if (et.Source.Equals("Deveel.Data.Net.CommitFaultException"))
					throw new CommitFaultException(et.Message);

				throw new Exception(et.Message);
			}

			// Return the DataAddress of the result transaction,
			return (DataAddress) response.Arguments[0].Value;
		}

		public string[] FindAllPaths() {
			IMessageProcessor processor = connector.Connect(managerAddress, ServiceType.Manager);
			RequestMessage request = new RequestMessage("getAllPaths");
			ResponseMessage response = processor.Process(request);
			if (response.HasError) {
				logger.Error("An error occurred in 'getAllPath': " + response.ErrorMessage);
				throw new Exception(response.ErrorMessage);
			}
			
			return (string[])response.Arguments[0].Value;
		}

		public DataAddress GetSnapshot(IServiceAddress root_server, String name) {
			IMessageProcessor processor = connector.Connect(root_server, ServiceType.Root);
			RequestMessage request = new RequestMessage("getSnapshot");
			request.Arguments.Add(name);
			ResponseMessage response = processor.Process(request);
			if (response.HasError)
				throw new Exception(response.ErrorMessage);

			return (DataAddress) response.Arguments[0].Value;
		}

		public DataAddress[] GetSnapshots(IServiceAddress rootServer, string name, DateTime timeStart, DateTime timeEnd) {
			IMessageProcessor processor = connector.Connect(rootServer, ServiceType.Root);
			RequestMessage request = new RequestMessage("getSnapshots");
			request.Arguments.Add(name);
			request.Arguments.Add(timeStart.ToBinary());
			request.Arguments.Add(timeEnd.ToBinary());
			ResponseMessage response = processor.Process(request);
			if (response.HasError)
				throw new Exception(response.ErrorMessage);

			return (DataAddress[]) response.Arguments[0].Value;
		}

		#region Implementation of ITreeStorageSystem
		
		public int MaxLeafByteSize {
			get { return 6134; }
		}
		
		public int MaxBranchSize {
			get { return 14; }
		}
		
		public long NodeHeapMaxSize {
			get { return max_transaction_node_heap_size; }
		}

		public void CheckPoint() {
			// This is a 'no-op' for the network system. This is called when a cache
			// flush occurs, so one idea might be to use this as some sort of hint?
		}

		public IList<ITreeNode> FetchNodes(long[] nids) {
			// The number of nodes,
			int node_count = nids.Length;
			// The array of read nodes,
			ITreeNode[] result_nodes = new ITreeNode[node_count];

			// Resolve special nodes first,
			{
				int i = 0;
				foreach (long nodeId in nids) {
					if ((nodeId & 0x01000000000000000L) != 0)
						result_nodes[i] = SparseLeafNode.Create(nodeId);

					++i;
				}
			}

			// Group all the nodes to the same block,
			List<long> uniqueBlocks = new List<long>();
			List<List<long>> uniqueBlockList = new List<List<long>>();
			{
				int i = 0;
				foreach (long node_ref in nids) {
					// If it's not a special node,
					if ((node_ref & 0x01000000000000000L) == 0) {
						// Get the block id and add it to the list of unique blocks,
						DataAddress address = new DataAddress(node_ref);
						// Check if the node is in the local cache,
						ITreeNode node = networkCache.GetNode(address);
						if (node != null) {
							result_nodes[i] = node;
						} else {
							// Not in the local cache so we need to bundle this up in a node
							// request on the block servers,
							// Group this node request by the block identifier
							long blockId = address.BlockId;
							int ind = uniqueBlocks.IndexOf(blockId);
							if (ind == -1) {
								ind = uniqueBlocks.Count;
								uniqueBlocks.Add(blockId);
								uniqueBlockList.Add(new List<long>());
							}
							List<long> blist = uniqueBlockList[ind];
							blist.Add(node_ref);
						}
					}
					++i;
				}
			}

			// Exit early if no blocks,
			if (uniqueBlocks.Count == 0)
				return result_nodes;

			// Resolve server records for the given block identifiers,
			IDictionary<long, IList<BlockServerElement>> servers_map = GetServersForBlock(uniqueBlocks);

			// The result nodes list,
			List<ITreeNode> nodes = new List<ITreeNode>();

			// For each unique block list,
			foreach (List<long> blist in uniqueBlockList) {
				// Make a block server request for each node in the block,
				RequestMessageStream block_server_msg = new RequestMessageStream();
				long block_id = -1;
				foreach (long node_ref in blist) {
					DataAddress address = new DataAddress(node_ref);
					RequestMessage request = new RequestMessage("readFromBlock");
					request.Arguments.Add(address);
					block_server_msg.AddMessage(request);
					block_id = address.BlockId;
				}

				if (block_id == -1)
					throw new ApplicationException("block_id == -1");

				// Get the shuffled list of servers the block is stored on,
				IList<BlockServerElement> servers = servers_map[block_id];

				// Go through the servers one at a time to fetch the block,
				bool success = false;
				for (int z = 0; z < servers.Count && !success; ++z) {
					BlockServerElement server = servers[z];

					// If the server is up,
					if (server.IsStatusUp) {
						// Open a connection with the block server,
						IMessageProcessor block_server_proc = connector.Connect(server.Address, ServiceType.Block);
						ResponseMessageStream message_in = (ResponseMessageStream) block_server_proc.Process(block_server_msg);
						// DEBUG: ++networkCommCount;
						// DEBUG: ++networkFetchCommCount;

						bool is_error = false;
						bool severe_error = false;
						// Turn each none-error message into a node
						foreach (ResponseMessage m in message_in) {
							if (m.HasError) {
								// See if this error is a block read error. If it is, we don't
								// tell the manager server to lock this server out completely.
								bool is_block_read_error = m.Error.Source.Equals("Deveel.Data.Net.BlockReadException");
								if (!is_block_read_error) {
									// If it's something other than a block read error, we mark
									// this error as severe,
									severe_error = true;
								}
								is_error = true;
							} else if (!is_error) {
								// The reply contains the block of data read.
								NodeSet node_set = (NodeSet)m.Arguments[0].Value;

								// Decode the node items into node objects,
								IEnumerator<Node> item_iterator = node_set.GetEnumerator();

								while (item_iterator.MoveNext()) {
									// Get the node item,
									Node node_item = item_iterator.Current;

									long node_ref = node_item.Id;

									DataAddress address = new DataAddress(node_ref);
									// Wrap around a buffered DataInputStream for reading values
									// from the store.
									BinaryReader input = new BinaryReader(node_item.Input, Encoding.Unicode);
									short node_type = input.ReadInt16();

									ITreeNode read_node;

									// Is the node type a leaf node?
									if (node_type == LeafType) {
										// Read the key
										int leaf_size = input.ReadInt32();

										byte[] buf = ReadNodeAsBuffer(node_item);
										if (buf == null) {
											buf = new byte[leaf_size + 6];
											input.Read(buf, 6, leaf_size);
											// Technically, we could comment these next two lines out.
											ByteBuffer.WriteInt2(node_type, buf, 0);
											ByteBuffer.WriteInt4(leaf_size, buf, 2);
										}

										// Create a leaf that's mapped to this data
										read_node = new ByteArrayTreeLeaf(node_ref, buf); ;

									}
										// Is the node type a branch node?
									else if (node_type == BranchType) {
										// Note that the entire branch is loaded into memory,
										int child_data_size = input.ReadInt32();
										long[] data_arr = new long[child_data_size];
										for (int n = 0; n < child_data_size; ++n) {
											data_arr[n] = input.ReadInt64();
										}
										// Create the branch node,
										read_node = new TreeBranch(node_ref, data_arr, child_data_size);
									} else {
										throw new InvalidDataState("Unknown node type: " + node_type, address);
									}

									// Is the node already in the list? If so we don't add it.
									if (!IsInNodeList(node_ref, nodes)) {
										// Put the read node in the cache and add it to the 'nodes'
										// list.
										networkCache.SetNode(address, read_node);
										nodes.Add(read_node);
									}
								}
							}
						}

						// If there was no error while reading the result, we assume the node
						// requests were successfully read.
						if (is_error == false) {
							success = true;
						} else {
							if (severe_error) {
								// If this is an error, we need to report the failure to the
								// manager server,
								ReportBlockServerFailure(server.Address);
								// Remove the block id from the server list cache,
								networkCache.RemoveServers(block_id);
							} else {
								// Otherwise, not a severe error (probably a corrupt block on a
								// server), so shuffle the server list for this block_id so next
								// time there's less chance of hitting this bad block.
								IList<BlockServerElement> srvs = networkCache.GetServers(block_id);
								List<BlockServerElement> server_list = new List<BlockServerElement>();
								server_list.AddRange(srvs);
								CollectionsUtil.Shuffle(server_list);
								networkCache.SetServers(block_id, server_list, 15 * 60 * 1000);
							}
						}

					}
				}

				// If the nodes were not successfully read, we generate an exception,
				if (!success) {
					// Remove from the cache,
					networkCache.RemoveServers(block_id);
					throw new ApplicationException("Unable to fetch node from block server");
				}
			}

			int sz = nodes.Count;
			if (sz == 0)
				throw new ApplicationException("Empty nodes list");

			for (int i = 0; i < sz; ++i) {
				ITreeNode node = nodes[i];
				long node_ref = node.Id;
				for (int n = 0; n < nids.Length; ++n) {
					if (nids[n] == node_ref)
						result_nodes[n] = node;
				}
			}

			// Check the result_nodes list is completely populated,
			for (int n = 0; n < result_nodes.Length; ++n) {
				if (result_nodes[n] == null)
					throw new ApplicationException("Assertion failed: result_nodes not completely populated.");
			}

			return result_nodes;
		}

		public bool IsNodeAvailable(long node_ref) {
			// Special node ref,
			if ((node_ref & 0x01000000000000000L) != 0) {
				return true;
			}
			// Check if it's in the local network cache
			DataAddress address = new DataAddress(node_ref);
			return (networkCache.GetNode(address) != null);
		}

		public bool LinkLeaf(Key key, long reference) {
			// NO-OP: A network tree system does not perform reference counting.
			//   Instead performs reachability testing and garbage collection through
			//   an external process.
			return true;
		}

		public void DisposeNode(long nid) {
			// NO-OP: Nodes can not be easily disposed, therefore this can do nothing
			//   except provide a hint to the garbage collector to reclaim resources
			//   on this node in the next cycle.
		}

		public ErrorStateException SetErrorState(Exception error) {
			//TODO: ERROR log ...
			errorState = new ErrorStateException(error);
			return errorState;
		}

		public void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}

		public IList<long> Persist(TreeWrite write) {
			return DoPersist(write, 1);
		}

		#endregion

		public void CreateReachabilityList(TextWriter warning_log, long node, IIndex node_list) {
			CheckErrorState();

			lock (reachability_lock) {
				reachability_tree_depth = -1;
				DoReachCheck(warning_log, node, node_list, 1);
			}
		}

		public TreeGraph CreateDiagnosticGraph(ITransaction t) {
			CheckErrorState();

			// The key object transaction
			Transaction ts = (Transaction)t;
			// Get the root node ref
			long root_node_ref = ts.RootNodeId;
			// Add the child node (the root node of the version graph).
			return CreateDiagnosticRootGraph(Key.Head, root_node_ref);
		}

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

		#region Transaction

		private class Transaction : TreeSystemTransaction {
			internal Transaction(ITreeSystem storeSystem, long versionId, DataAddress rootNode)
				: base(storeSystem, versionId, rootNode.Value, false) {
			}

			public Transaction(ITreeSystem tree_system, long version_id)
				: base(tree_system, version_id, -1, false) {
				SetToEmpty();
			}
		}

		#endregion

		#region InvalidDataState

		private sealed class InvalidDataState : ApplicationException {

			private readonly DataAddress address;

			public InvalidDataState(string message, DataAddress address)
				: base(message) {
				this.address = address;
			}

			public DataAddress Address {
				get { return address; }
			}

		}

		#endregion
	}
}