using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Text;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;
using Deveel.Data.Store;
using Deveel.Data.Util;

using TomKaminski;

namespace Deveel.Data.Net {
	internal class NetworkTreeSystem : ITreeSystem {
		internal NetworkTreeSystem(IServiceConnector connector, IServiceAddress[] managerAddresses, INetworkCache networkCache, ServiceStatusTracker statusTracker) {
			if (!(connector.MessageSerializer is IMessageStreamSupport))
				throw new ArgumentException("The message serializer specified by the connector is not valid (must be RPC).");
			
			this.connector = connector;
			this.managerAddresses = managerAddresses;
			this.networkCache = networkCache;
			this.statusTracker = statusTracker;
			failures = new Dictionary<IServiceAddress, DateTime>();
			failuresBidc = new Dictionary<IServiceAddress, DateTime>();
			
			logger = Logger.Network;
		}

		private readonly IServiceConnector connector;
		private readonly IServiceAddress[] managerAddresses;
		private readonly INetworkCache networkCache;
		private readonly ServiceStatusTracker statusTracker;

		private readonly Dictionary<IServiceAddress, DateTime> failures;
		private readonly Dictionary<IServiceAddress, DateTime> failuresBidc;

		private readonly Dictionary<IServiceAddress, int> proximityMap = new Dictionary<IServiceAddress, int>();

		private readonly Object reachabilityLock = new Object();
		private int reachability_tree_depth;

		private long maxTransactionNodeHeapSize;

		private ErrorStateException errorState;
		private readonly Logger logger;

		private const short LeafType = 0x019e0;
		private const short BranchType = 0x022e0;

		private PathInfo GetPathInfoFor(String path_name) {
			PathInfo path_info = networkCache.GetPathInfo(path_name);
			if (path_info == null) {
				// Path info not found in the cache, so query the manager cluster for the
				// info.

				RequestMessage request = new RequestMessage("getPathInfoForPath");
				request.Arguments.Add(path_name);

				Message response = ProcessManager(request);

				if (response.HasError) {
					logger.Error(String.Format("Error while executing 'getPathInfoForPath' command: {0}", response.ErrorMessage));
					logger.Error(response.Error.StackTrace);
					throw new ApplicationException(response.ErrorMessage);
				}
	
				path_info = (PathInfo)response.Arguments[0].Value;

				if (path_info == null)
					throw new ApplicationException("Path not found: " + path_name);

				// Put it in the local cache,
				networkCache.SetPathInfo(path_name, path_info);
			}

			return path_info;
		}

		private void NotifyAllManagers(Message request) {
			Message[] responses = new Message[managerAddresses.Length];
			for (int i = 0; i < managerAddresses.Length; ++i) {
				IServiceAddress manager = managerAddresses[i];
				if (statusTracker.IsServiceUp(manager, ServiceType.Manager)) {
					IMessageProcessor processor = connector.Connect(manager, ServiceType.Manager);
					responses[i] = processor.Process(request);
				}
			}

			for (int i = 0; i < managerAddresses.Length; ++i) {
				Message response = responses[i];
				if (response != null) {
					IServiceAddress manager = managerAddresses[i];
					// If any errors happened,
					// If it's a connection fail message, we try connecting to another
					// manager.
					if (IsConnectionFailMessage(response)) {
						// Tell the tracker it's down,
						statusTracker.ReportServiceDownClientReport(manager, ServiceType.Manager);
						break;
					}

				}
			}
		}

		private void ReportBlockServerFailure(IServiceAddress address) {
			// Report the failure,
			logger.Warning(String.Format("Reporting failure for {0} to manager server", address));

			// Failure throttling,
			lock (failures) {
				DateTime current_time = DateTime.Now;
				DateTime last_address_fail_time;
				if (failures.TryGetValue(address, out last_address_fail_time) &&
				    last_address_fail_time.AddSeconds(30) > current_time) {
					// We don't respond to failure notifications on the same address if a
					// failure notice arrived within a minute of the last one accepted.
					return;
				}
				failures[address] = current_time;
			}

			RequestMessage message_out = new RequestMessage("notifyBlockServerFailure");
			message_out.Arguments.Add(address);

			// Process the failure report message on the manager server,
			NotifyAllManagers(message_out);
		}

		private void ReportBlockIdCorruption(IServiceAddress blockServer, BlockId blockId, string failType) {
			// Report the failure,
			logger.Warning(String.Format("Reporting a data failure (type = {0}) for block {1} at block server {2}", failType, blockId, blockServer));

			// Failure throttling,
			lock (failuresBidc) {
				DateTime currentTime = DateTime.Now;
				DateTime lastAddressFailTime;
				if (failuresBidc.TryGetValue(blockServer, out lastAddressFailTime) &&
				    lastAddressFailTime.AddSeconds(10) > currentTime) {
					// We don't respond to failure notifications on the same address if a
					// failure notice arrived within a minute of the last one accepted.
					return;
				}

				failuresBidc[blockServer] = currentTime;
			}

			RequestMessage request = new RequestMessage("notifyBlockIdCorruption");
			request.Arguments.Add(blockServer);
			request.Arguments.Add(blockId);
			request.Arguments.Add(failType);

			// Process the failure report message on the manager server,
			// (Ignore any error message generated)
			ProcessManager(request);
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
						logger.Error("Cannot determine the proximity factor for address " + node, e);
						closeness = Int32.MaxValue;
					}

					// Put it in the map,
					proximityMap.Add(node, closeness);
				}
				return closeness;
			}
		}

		private IDictionary<BlockId, IList<BlockServerElement>> GetServersForBlock(IList<BlockId> blockIds) {
			// The result map,
			Dictionary<BlockId, IList<BlockServerElement>> resultMap = new Dictionary<BlockId, IList<BlockServerElement>>();

			List<BlockId> noneCached = new List<BlockId>(blockIds.Count);
			foreach (BlockId blockId in blockIds) {
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

			MessageStream message_out = new MessageStream(MessageType.Request);

			foreach (BlockId block_id in noneCached) {
				RequestMessage request = new RequestMessage("getServerListForBlock");
				request.Arguments.Add(block_id);
				message_out.AddMessage(request);
			}

			// Process a command on the manager,
			MessageStream message_in = (MessageStream) ProcessManager(message_out);

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
				BlockId block_id = noneCached[n];
				resultMap.Add(block_id, srvs);
				// Add it to the cache,
				// NOTE: TTL hard-coded to 15 minute
				networkCache.SetServers(block_id, srvs, 15*60*1000);

				++n;
			}

			// Return the list
			return resultMap;
		}

		private static bool IsInNodeList(NodeId reference, IList<ITreeNode> nodes) {
			foreach (ITreeNode node in nodes) {
				if (reference.Equals(node.Id))
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

		private TreeGraph CreateDiagnosticRootGraph(Key left_key, NodeId reference) {

			// The node being returned
			TreeGraph node;

			// Fetch the node,
			ITreeNode tree_node = FetchNodes(new NodeId[] { reference })[0];

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
					NodeId child_ref = branch.GetChild(i);
					// If the ref is a special node, skip it
					if (child_ref.IsSpecial) {
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

		private void DoReachCheck(TextWriter warningLog, NodeId node, IIndex nodeList, int curDepth) {
			throw new NotImplementedException();
		}

		private List<NodeId> DoPersist(TreeWrite sequence, int tryCount) {
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
			NodeId[] outRefs = new NodeId[sz];

			MessageStream allocateMessage = new MessageStream(MessageType.Request);

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

			// Process a command on the manager,
			MessageStream resultStream = (MessageStream) ProcessManager(allocateMessage);

			// The unique list of blocks,
			List<BlockId> uniqueBlocks = new List<BlockId>();

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

			IDictionary<BlockId, IList<BlockServerElement>> blockToServerMap = GetServersForBlock(uniqueBlocks);

			// Make message streams for each unique block
			int ubid_count = uniqueBlocks.Count;
			MessageStream[] ubidStream = new MessageStream[ubid_count];
			for (int i = 0; i < ubidStream.Length; ++i) {
				ubidStream[i] = new MessageStream(MessageType.Request);
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
						NodeId childRef = branch.GetChild(o);
						if (childRef.IsInMemory) {
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
					dout.Write((short)0);	// Reserved for future
					dout.Write(0);			// The crc32 checksum will be written here,
					dout.Write(ndsz);
					for (int o = 0; o < ndsz; ++o) {
						dout.Write(nodeData[o]);
					}
					dout.Flush();

					// Turn it into a byte array,
					nodeBuf = bout.ToArray();

					// Write the crc32 of the data,
					Crc32 checksum = new Crc32();
					checksum.ComputeHash(nodeBuf, 8, nodeBuf.Length - 8);
					ByteBuffer.WriteInt4((int)checksum.CrcValue, nodeBuf, 4);

					// Put this branch into the local cache,
					networkCache.SetNode(refs[i], branch);

				} else {
					// If it's a leaf node,

					TreeLeaf leaf = (TreeLeaf)node;
					int lfsz = leaf.Length;

					nodeBuf = new byte[lfsz + 12];

					// Format the data,
					ByteBuffer.WriteInt2(LeafType, nodeBuf, 0);
					ByteBuffer.WriteInt2(0, nodeBuf, 2);  // Reserved for future
					ByteBuffer.WriteInt4(lfsz, nodeBuf, 8);
					leaf.Read(0, nodeBuf, 12, lfsz);

					// Calculate and set the checksum,
					Crc32 checksum = new Crc32();
					checksum.ComputeHash(nodeBuf, 8, nodeBuf.Length - 8);
					ByteBuffer.WriteInt4((int)checksum.CrcValue, nodeBuf, 4);

					// Put this leaf into the local cache,
					leaf = new ByteArrayTreeLeaf(refs[i].Value, nodeBuf);
					networkCache.SetNode(refs[i], leaf);

				}

				// The DataAddress this node is being written to,
				DataAddress address = refs[i];
				// Get the block id,
				BlockId blockId = address.BlockId;
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
				MessageStream requestMessageStream = ubidStream[i];

				// Get the servers this message needs to be sent to,
				BlockId block_id = uniqueBlocks[i];
				IList<BlockServerElement> blockServers = blockToServerMap[block_id];

				// Format a message for writing this node out,
				int bssz = blockServers.Count;
				IMessageProcessor[] blockServerProcs = new IMessageProcessor[bssz];

				// Make the block server connections,
				for (int o = 0; o < bssz; ++o) {
					IServiceAddress address = blockServers[o].Address;
					blockServerProcs[o] = connector.Connect(address, ServiceType.Block);
					MessageStream responseMessageStream = (MessageStream) blockServerProcs[o].Process(requestMessageStream);
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
							MessageStream toRollback = (MessageStream) successProcess[p + 1];

							List<DataAddress> rollbackNodes = new List<DataAddress>(128);
							foreach(Message rm in toRollback) {
								DataAddress raddr = (DataAddress) rm.Arguments[0].Value;
								rollbackNodes.Add(raddr);
							}

							// Create the rollback message,
							RequestMessage rollbackRequest = new RequestMessage("rollbackNodes");
							rollbackRequest.Arguments.Add(rollbackNodes.ToArray());

							// Send it to the block server,
							Message responseMessage = connector.Connect(blockAddress, ServiceType.Block).Process(rollbackRequest);
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
			return new List<NodeId>(outRefs);

		}

		internal void SetMaxNodeCacheHeapSize(long value) {
			maxTransactionNodeHeapSize = value;
		}

		public DataAddress CreateEmptyDatabase() {
			// The child reference is a sparse node element
			NodeId child_ref = NodeId.CreateSpecialSparseNode(1, 4);

			// Create a branch,
			TreeBranch root_branch = new TreeBranch(NodeId.CreateInMemoryNode(0L), MaxBranchSize);
			root_branch.Set(child_ref, 4, Key.Tail, child_ref, 4);

			TreeWrite seq = new TreeWrite();
			seq.NodeWrite(root_branch);
			IList<NodeId> refs = Persist(seq);

			// The written root node reference,
			NodeId rootId = refs[0];

			// Return the root,
			return new DataAddress(rootId);
		}

		private static bool IsConnectionFailMessage(Message m) {
			// TODO: This should detect comm failure rather than catch-all.
			if (m.HasError) {
				MessageError error = m.Error;
				string errorSource = error.Source;
				// If it's a connect exception,
				//TODO: is it there a more specific exception to caught?
				if (errorSource.Equals("System.Net.Sockets.SocketException"))
					return true;
				if (errorSource.Equals("Deveel.Data.Net.ServiceNotConnectedException"))
					return true;
			}

			return false;
		}

		private Message ProcessManager(Message request) {

			// We go through all the manager addresses from first to last until we
			// find one that is currently up,

			// This uses a service status tracker object maintained by the network
			// cache to keep track of manager servers that aren't operational.

			Message response = null;
			for (int i = 0; i < managerAddresses.Length; ++i) {
				IServiceAddress manager = managerAddresses[i];
				if (statusTracker.IsServiceUp(manager, ServiceType.Manager)) {
					IMessageProcessor processor = connector.Connect(manager, ServiceType.Manager);
					response = processor.Process(request);

					bool failed = false;
					// if this is a MessageStream
					if (response is MessageStream) {
						// If any errors happened,
						foreach (Message m in ((MessageStream) response)) {
							// If it's a connection fail message, we try connecting to another
							// manager.
							if (IsConnectionFailMessage(m)) {
								// Tell the tracker it's down,
								statusTracker.ReportServiceDownClientReport(manager, ServiceType.Manager);
								failed = true;
								break;
							}
						}
					} else {
						// If it's a connection fail message, we try connecting to another
						// manager.
						if (IsConnectionFailMessage(response)) {
							// Tell the tracker it's down,
							statusTracker.ReportServiceDownClientReport(manager, ServiceType.Manager);
							failed = true;
						}
					}

					if (!failed)
						return response;
				}
			}

			// If we didn't even try one, we test the first manager.
			if (response == null) {
				IMessageProcessor processor = connector.Connect(managerAddresses[0], ServiceType.Manager);
				response = processor.Process(request);
			}

			// All managers are currently down, so return the last msg_in,
			return response;
		}

		private Message ProcessSingleRoot(Message request, IServiceAddress root_server) {
			IMessageProcessor processor = connector.Connect(root_server, ServiceType.Root);
			Message response = processor.Process(request);
			Message last_m = null;

			if (response is MessageStream) {
				foreach (Message m in (MessageStream) response) {
					last_m = m;
					if (m.HasError) {
						// If it's a connection failure, inform the service tracker and throw
						// service not available exception.
						if (IsConnectionFailMessage(m)) {
							statusTracker.ReportServiceDownClientReport(root_server, ServiceType.Root);
							throw new ServiceNotConnectedException(root_server.ToString());
						}

						string errorSource = m.Error.Source;
						// Rethrow InvalidPathInfoException locally,
						if (errorSource.Equals("Deveel.Data.Net.InvalidPathInfoException"))
							throw new InvalidPathInfoException(m.ErrorMessage);
					}
				}
			} else {
				last_m = response;

				if (last_m.HasError) {
					// If it's a connection failure, inform the service tracker and throw
					// service not available exception.
					if (IsConnectionFailMessage(last_m)) {
						statusTracker.ReportServiceDownClientReport(root_server, ServiceType.Root);
						throw new ServiceNotConnectedException(root_server.ToString());
					}

					string errorSource = last_m.Error.Source;
					// Rethrow InvalidPathInfoException locally,
					if (errorSource.Equals("Deveel.Data.Net.InvalidPathInfoException"))
						throw new InvalidPathInfoException(last_m.ErrorMessage);
				}
			}

			return last_m;
		}

		public ITransaction CreateTransaction(DataAddress rootNode) {
			return new Transaction(this, 0, rootNode);
		}

		public ITransaction CreateEmptyTransaction() {
			return new Transaction(this, 0);
		}

		public DataAddress FlushTransaction(ITransaction transaction) {
			Transaction netTransaction = (Transaction)transaction;

			try {
				netTransaction.CheckOut();
				return new DataAddress(netTransaction.RootNodeId);
			} catch (IOException e) {
				throw new Exception(e.Message, e);
			}
		}

		public void DisposeTransaction(ITransaction transaction) {
			((Transaction)transaction).Dispose();
		}

		public DataAddress Commit(string pathName, DataAddress proposal) {
			// Get the PathInfo object for the given path name,
			PathInfo path_info = GetPathInfoFor(pathName);

			// We can only commit on the root leader,
			IServiceAddress root_server = path_info.RootLeader;
			try {
				// TODO: If the root leader is not available, we need to go through
				//   a new leader election process.

				Message request = new RequestMessage("commit");
				request.Arguments.Add(pathName);
				request.Arguments.Add(path_info.Version);
				request.Arguments.Add(proposal);

				Message response = ProcessSingleRoot(request, root_server);

				if (response.HasError) {
					// Rethrow commit fault locally,
					if (response.Error.Source.Equals("Deveel.Data.Net.CommitFaultException")) {
						throw new CommitFaultException(response.ErrorMessage);
					}
						
					throw new ApplicationException(response.ErrorMessage);
				}

				// Return the DataAddress of the result transaction,
				return (DataAddress)response.Arguments[0].Value;
			} catch (InvalidPathInfoException e) {
				// Clear the cache and requery the manager server for a new path info,
				networkCache.SetPathInfo(pathName, null);
				return Commit(pathName, proposal);
			}
		}

		public string[] FindAllPaths() {
			RequestMessage request = new RequestMessage("getAllPaths");

			// Process a command on the manager,
			Message response = ProcessManager(request);

			if (response.HasError) {
				logger.Error("'getAllPaths' command failed: {0}", response.ErrorMessage);
				logger.Error(response.ErrorStackTrace);
				throw new ApplicationException(response.ErrorMessage);
			}
				
			return (string[]) response.Arguments[0].Value;
		}

		public string GetPathType(string pathName) {
			PathInfo pathInfo = GetPathInfoFor(pathName);
			return pathInfo.PathType;
		}

		private DataAddress DoGetSnapshot(PathInfo path_info, IServiceAddress root_server) {
			RequestMessage msg_out = new RequestMessage("getSnapshot");
			msg_out.Arguments.Add(path_info.PathName);
			msg_out.Arguments.Add(path_info.Version);

			Message m = ProcessSingleRoot(msg_out, root_server);
			if (m.HasError) {
				throw new ApplicationException(m.ErrorMessage);
			}
			return (DataAddress)m.Arguments[0].Value;
		}

		public DataAddress GetSnapshot(string pathName) {
			// Get the PathInfo object for the given path name,
			PathInfo path_info = GetPathInfoFor(pathName);

			// Try the root leader first,
			IServiceAddress root_server = path_info.RootLeader;
			try {
				DataAddress data_address = DoGetSnapshot(path_info, root_server);

				// TODO: if the root leader is not available, query the replicated
				//   root servers with this path.

				return data_address;
			} catch (InvalidPathInfoException e) {
				// Clear the cache and requery the manager server for a new path info,
				networkCache.SetPathInfo(pathName, null);
				return GetSnapshot(pathName);
			}
		}

		private DataAddress[] DoGetSnapshots(PathInfo pathInfo, IServiceAddress server, DateTime timeStart, DateTime timeEnd) {
			RequestMessage request = new RequestMessage("getSnapshots");
			request.Arguments.Add(pathInfo.PathName);
			request.Arguments.Add(pathInfo.Version);
			request.Arguments.Add(timeStart.ToUniversalTime().ToBinary());
			request.Arguments.Add(timeEnd.ToUniversalTime().ToBinary());

			Message m = ProcessSingleRoot(request, server);
			if (m.HasError)
				throw new ApplicationException(m.ErrorMessage);

			return (DataAddress[])m.Arguments[0].Value;

		}

		public DataAddress[] GetSnapshots(string pathName, DateTime timeStart, DateTime timeEnd) {
			// Get the PathInfo object for the given path name,
			PathInfo pathInfo = GetPathInfoFor(pathName);

			// Try the root leader first,
			IServiceAddress rootServer = pathInfo.RootLeader;
			try {
				DataAddress[] dataAddresses = DoGetSnapshots(pathInfo, rootServer, timeStart, timeEnd);

				// TODO: if the root leader is not available, query the replicated
				//   root servers with this path.

				return dataAddresses;
			} catch (InvalidPathInfoException) {
				// Clear the cache and requery the manager server for a new path info,
				networkCache.SetPathInfo(pathName, null);
				return GetSnapshots(pathName, timeStart, timeEnd);
			}
		}

		#region Implementation of ITreeStorageSystem
		
		public int MaxLeafByteSize {
			get { return 6134; }
		}
		
		public int MaxBranchSize {
			get { return 14; }
		}
		
		public long NodeHeapMaxSize {
			get { return maxTransactionNodeHeapSize; }
		}

		public bool NotifyForAllNodes {
			get {
				// We don't need to account for all references and disposes to nodes in
				// this implementation.
				return false;
			}
		}

		public void CheckPoint() {
			// This is a 'no-op' for the network system. This is called when a cache
			// flush occurs, so one idea might be to use this as some sort of hint?
		}

		public IList<ITreeNode> FetchNodes(NodeId[] nids) {
			// The number of nodes,
			int nodeCount = nids.Length;
			// The array of read nodes,
			ITreeNode[] resultNodes = new ITreeNode[nodeCount];

			// Resolve special nodes first,
			{
				int i = 0;
				foreach (NodeId node_ref in nids) {
					if (node_ref.IsSpecial) {
						resultNodes[i] = node_ref.CreateSpecialTreeNode();
					}
					++i;
				}
			}

			// Group all the nodes to the same block,
			List<BlockId> uniqueBlocks = new List<BlockId>();
			List<List<NodeId>> uniqueBlockList = new List<List<NodeId>>();
			{
				int i = 0;
				foreach (NodeId node_ref in nids) {
					// If it's not a special node,
					if (!node_ref.IsSpecial) {
						// Get the block id and add it to the list of unique blocks,
						DataAddress address = new DataAddress(node_ref);
						// Check if the node is in the local cache,
						ITreeNode node = networkCache.GetNode(address);
						if (node != null) {
							resultNodes[i] = node;
						} else {
							// Not in the local cache so we need to bundle this up in a node
							// request on the block servers,
							// Group this node request by the block identifier
							BlockId block_id = address.BlockId;
							int ind = uniqueBlocks.IndexOf(block_id);
							if (ind == -1) {
								ind = uniqueBlocks.Count;
								uniqueBlocks.Add(block_id);
								uniqueBlockList.Add(new List<NodeId>());
							}
							List<NodeId> blist = uniqueBlockList[ind];
							blist.Add(node_ref);
						}
					}
					++i;
				}
			}

			// Exit early if no blocks,
			if (uniqueBlocks.Count == 0) {
				return resultNodes;
			}

			// Resolve server records for the given block identifiers,
			IDictionary<BlockId, IList<BlockServerElement>> serversMap = GetServersForBlock(uniqueBlocks);

			// The result nodes list,
			List<ITreeNode> nodes = new List<ITreeNode>();

			// Checksumming objects
			byte[] checksum_buf = null;
			Crc32 crc32 = null;

			// For each unique block list,
			foreach (List<NodeId> blist in uniqueBlockList) {
				// Make a block server request for each node in the block,
				MessageStream block_server_msg = new MessageStream(MessageType.Request);
				BlockId block_id = null;
				foreach (NodeId node_ref in blist) {
					DataAddress address = new DataAddress(node_ref);
					RequestMessage message = new RequestMessage("readFromBlock");
					message.Arguments.Add(address);
					block_id = address.BlockId;
					block_server_msg.AddMessage(message);
				}

				if (block_id == null) {
					throw new ApplicationException("block_id == null");
				}

				// Get the shuffled list of servers the block is stored on,
				IList<BlockServerElement> servers = serversMap[block_id];

				// Go through the servers one at a time to fetch the block,
				bool success = false;
				for (int z = 0; z < servers.Count && !success; ++z) {
					BlockServerElement server = servers[z];
					// If the server is up,
					if (server.IsStatusUp) {

						// Open a connection with the block server,
						IMessageProcessor blockServerProc = connector.Connect(server.Address, ServiceType.Block);
						MessageStream response = (MessageStream) blockServerProc.Process(block_server_msg);
						//DEBUG: ++networkCommCount;
						//DEBUG: ++networkFetchCommCount;

						bool is_error = false;
						bool severe_error = false;
						bool crc_error = false;
						bool connection_error = false;

						// Turn each none-error message into a node
						foreach (Message m in response) {
							if (m.HasError) {
								// See if this error is a block read error. If it is, we don't
								// tell the manager server to lock this server out completely.
								bool is_block_read_error = m.Error.Source.Equals("Deveel.Data.Net.BlockReadException");
								// If it's a connection fault,
								if (IsConnectionFailMessage(m)) {
									connection_error = true;
								} else if (!is_block_read_error) {
									// If it's something other than a block read error or
									// connection failure, we set the severe flag,
									severe_error = true;
								}
								is_error = true;
							} else if (!is_error) {
								// The reply contains the block of data read.
								NodeSet nodeSet = (NodeSet) m.Arguments[0].Value;

								DataAddress address = null;

								// Catch any IOExceptions (corrupt zips, etc)
								try {
									// Decode the node items into Java node objects,
									foreach (Node node_item in nodeSet) {
										NodeId nodeId = node_item.Id;

										address = new DataAddress(nodeId);
										// Wrap around a buffered DataInputStream for reading values
										// from the store.
										BinaryReader input = new BinaryReader(node_item.Input);
										short node_type = input.ReadInt16();

										ITreeNode read_node = null;

										if (crc32 == null)
											crc32 = new Crc32();

										crc32.Initialize();

										// Is the node type a leaf node?
										if (node_type == LeafType) {
											// Read the checksum,
											input.ReadInt16(); // For future use...
											int checksum = input.ReadInt32();
											// Read the size
											int leaf_size = input.ReadInt32();

											byte[] buf = ReadNodeAsBuffer(node_item);
											if (buf == null) {
												buf = new byte[leaf_size + 12];
												ByteBuffer.WriteInt4(leaf_size, buf, 8);
												input.Read(buf, 12, leaf_size);
											}

											// Check the checksum...
											crc32.ComputeHash(buf, 8, leaf_size + 4);
											int calc_checksum = (int) crc32.CrcValue;
											if (checksum != calc_checksum) {
												// If there's a CRC failure, we reject his node,
												logger.Warning(String.Format("CRC failure on node {0} @ {1}", nodeId, server.Address));
												is_error = true;
												crc_error = true;
												// This causes the read to retry on a different server
												// with this block id
											} else {
												// Create a leaf that's mapped to this data
												ITreeNode leaf = new ByteArrayTreeLeaf(nodeId, buf);
												read_node = leaf;
											}

										}
											// Is the node type a branch node?
										else if (node_type == BranchType) {
											// Read the checksum,
											input.ReadInt16(); // For future use...
											int checksum = input.ReadInt32();

											// Check the checksum objects,
											if (checksum_buf == null) 
												checksum_buf = new byte[8];

											// Note that the entire branch is loaded into memory,
											int child_data_size = input.ReadInt32();
											ByteBuffer.WriteInt4(child_data_size, checksum_buf, 0);
											crc32.ComputeHash(checksum_buf, 0, 4);
											long[] data_arr = new long[child_data_size];
											for (int n = 0; n < child_data_size; ++n) {
												long item = input.ReadInt64();
												ByteBuffer.WriteInt8(item, checksum_buf, 0);
												crc32.ComputeHash(checksum_buf, 0, 8);
												data_arr[n] = item;
											}

											// The calculated checksum value,
											int calc_checksum = (int) crc32.CrcValue;
											if (checksum != calc_checksum) {
												// If there's a CRC failure, we reject his node,
												logger.Warning(String.Format("CRC failure on node {0} @ {1}", nodeId, server.Address));
												is_error = true;
												crc_error = true;
												// This causes the read to retry on a different server
												// with this block id
											} else {
												// Create the branch node,
												TreeBranch branch =
													new TreeBranch(nodeId, data_arr, child_data_size);
												read_node = branch;
											}

										} else {
											logger.Error(String.Format("Unknown node {0} type: {1}", address, node_type));
											is_error = true;
										}

										// Is the node already in the list? If so we don't add it.
										if (read_node != null && !IsInNodeList(nodeId, nodes)) {
											// Put the read node in the cache and add it to the 'nodes'
											// list.
											networkCache.SetNode(address, read_node);
											nodes.Add(read_node);
										}
									}

								} catch (IOException e) {
									// This catches compression errors, as well as any other misc
									// IO errors.
									if (address != null) {
										logger.Error(String.Format("IO Error reading node {0}", address));
									}
									logger.Error(e.Message, e);
									is_error = true;
								}
							}

						}

						// If there was no error while reading the result, we assume the node
						// requests were successfully read.
						if (is_error == false) {
							success = true;
						} else {
							// If this is a connection failure, we report the block failure.
							if (connection_error) {
								// If this is an error, we need to report the failure to the
								// manager server,
								ReportBlockServerFailure(server.Address);
								// Remove the block id from the server list cache,
								networkCache.RemoveServers(block_id);
							} else {
								String fail_type = "General";
								if (crc_error) {
									fail_type = "CRC Failure";
								} else if (severe_error) {
									fail_type = "Exception during process";
								}

								// Report to the first manager the block failure, so it may
								// investigate and hopefully correct.
								ReportBlockIdCorruption(server.Address, block_id, fail_type);

								// Otherwise, not a severe error (probably a corrupt block on a
								// server), so shuffle the server list for this block_id so next
								// time there's less chance of hitting this bad block.
								IList<BlockServerElement> srvs = networkCache.GetServers(block_id);
								if (srvs != null) {
									List<BlockServerElement> server_list = new List<BlockServerElement>();
									server_list.AddRange(srvs);
									CollectionsUtil.Shuffle(server_list);
									networkCache.SetServers(block_id, server_list, 15*60*1000);
								}
							}

							// We will now go retry the query on the next block server,
						}
					}
				}

				// If the nodes were not successfully read, we generate an exception,
				if (!success) {
					// Remove from the cache,
					networkCache.RemoveServers(block_id);
					throw new ApplicationException("Unable to fetch node from a block server" + " (block = " + block_id + ")");
				}

			}

			int sz = nodes.Count;
			if (sz == 0)
				throw new ApplicationException("Empty nodes list");

			for (int i = 0; i < sz; ++i) {
				ITreeNode node = nodes[i];
				NodeId node_ref = node.Id;
				for (int n = 0; n < nids.Length; ++n) {
					if (nids[n].Equals(node_ref)) {
						resultNodes[n] = node;
					}
				}
			}

			// Check the result_nodes list is completely populated,
			for (int n = 0; n < resultNodes.Length; ++n) {
				if (resultNodes[n] == null)
					throw new ApplicationException("Assertion failed: result node list not completely populated.");
			}

			return resultNodes;
		}

		public bool IsNodeAvailable(NodeId nodeId) {
			// Special node ref,
			if (nodeId.IsSpecial)
				return true;

			// Check if it's in the local network cache
			DataAddress address = new DataAddress(nodeId);
			return (networkCache.GetNode(address) != null);
		}

		public bool LinkLeaf(Key key, NodeId nodeId) {
			// NO-OP: A network tree system does not perform reference counting.
			//   Instead performs reachability testing and garbage collection through
			//   an external process.
			return true;
		}

		public void DisposeNode(NodeId nid) {
			// NO-OP: Nodes can not be easily disposed, therefore this can do nothing
			//   except provide a hint to the garbage collector to reclaim resources
			//   on this node in the next cycle.
		}

		public ErrorStateException SetErrorState(Exception error) {
			Logger.Network.Error("Entering error state", error);
			errorState = new ErrorStateException(error);
			return errorState;
		}

		public void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}

		public IList<NodeId> Persist(TreeWrite write) {
			return DoPersist(write, 1);
		}

		#endregion

		public void CreateReachabilityList(TextWriter warning_log, NodeId node, IIndex node_list) {
			CheckErrorState();

			lock (reachabilityLock) {
				reachability_tree_depth = -1;
				DoReachCheck(warning_log, node, node_list, 1);
			}
		}

		public TreeGraph CreateDiagnosticGraph(ITransaction t) {
			CheckErrorState();

			// The key object transaction
			Transaction ts = (Transaction)t;
			// Get the root node ref
			NodeId rootNodeId = ts.RootNodeId;
			// Add the child node (the root node of the version graph).
			return CreateDiagnosticRootGraph(Key.Head, rootNodeId);
		}

		#region MemoryTreeLeaf

		private sealed class ByteArrayTreeLeaf : TreeLeaf {

			private readonly byte[] buffer;
			private readonly NodeId id;

			public ByteArrayTreeLeaf(NodeId id, byte[] buffer) {
				this.id = id;
				this.buffer = buffer;
			}

			// ---------- Implemented from TreeLeaf ----------

			public override NodeId Id {
				get { return id; }
			}

			public override int Length {
				get { return buffer.Length - 12; }
			}

			public override int Capacity {
				get { throw new Exception(); }
			}

			public override void Read(int position, byte[] buf, int off, int len) {
				Array.Copy(buffer, 12 + position, buf, off, len);
			}

			public override void WriteTo(IAreaWriter writer) {
				writer.Write(buffer, 12, Length);
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

			public Transaction(ITreeSystem treeSystem, long versionId)
				: base(treeSystem, versionId, null, false) {
				SetToEmpty();
			}
		}

		#endregion
	}
}