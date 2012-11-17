//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;
using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public sealed class NetworkTreeSystem : ITreeSystem {
		private const short StoreLeafType = 0x019e0;
		private const short StoreBranchType = 0x022e0;

		private readonly IServiceConnector connector;
		private readonly IServiceAddress[] managerAddresses;

		private readonly INetworkCache networkCache;

		private readonly ServiceStatusTracker serviceTracker;

		private readonly Dictionary<IServiceAddress, DateTime> failureFloodControl;
		private readonly Dictionary<IServiceAddress, DateTime> failureFloodControlBidc;

		private long maxTransactionNodeHeapSize = 32*1024*1024;

		public volatile int NetworkCommCount = 0;
		public volatile int NetworkFetchCommCount = 0;

		private readonly Logger log = Logger.Network;

		private ErrorStateException errorState;

		private readonly Dictionary<IServiceAddress, int> closenessMap = new Dictionary<IServiceAddress, int>();

		private readonly object reachabilityLock = new object();
		private int reachabilityTreeDepth;

		public NetworkTreeSystem(IServiceConnector connector, IServiceAddress[] managerAddresses,
		                         INetworkCache networkCache, ServiceStatusTracker serviceTracker) {
			this.connector = connector;
			this.managerAddresses = managerAddresses;
			this.networkCache = networkCache;
			this.serviceTracker = serviceTracker;

			failureFloodControl = new Dictionary<IServiceAddress, DateTime>();
			failureFloodControlBidc = new Dictionary<IServiceAddress, DateTime>();
		}

		public int MaxBranchSize {
			get {
				// TODO: Make this user-definable.
				// Remember though - you can't change this value on the fly so we'll need
				//   some central management on the network for configuration values.

				//    // Note: 25 results in a branch size of around 1024 in size when full so
				//    //   limits the maximum size of a branch to this size.
				return 14;
			}
		}

		public int MaxLeafByteSize {
			get {
				// TODO: Make this user-definable.
				// Remember though - you can't change this value on the fly so we'll need
				//   some central management on the network for configuration values.
				return 6134;
			}
		}

		public long NodeHeapMaxSize {
			get { return maxTransactionNodeHeapSize; }
			set { maxTransactionNodeHeapSize = value; }
		}

		public bool NotifyNodeChanged {
			get { return false; }
		}

		private static bool IsConnectionFailMessage(Message m) {
			// TODO: This should detect comm failure rather than catch-all.
			if (m.HasError) {
				MessageError error = m.Error;
				string source = error.Source;
				// If it's a connect exception,
				if (source.Equals("System.Net.Sockets.SocketException"))
					return true;
				if (source.Equals("Deveel.Data.Net.ServiceNotConnectedException"))
					return true;
			}

			return false;
		}

		private void NotifyAllManagers(MessageStream outputStream) {
			IEnumerable<Message>[] inputStreams = new IEnumerable<Message>[managerAddresses.Length];
			for (int i = 0; i < managerAddresses.Length; ++i) {
				IServiceAddress manager = managerAddresses[i];
				if (serviceTracker.IsServiceUp(manager, ServiceType.Manager)) {
					IMessageProcessor processor = connector.Connect(manager, ServiceType.Manager);
					inputStreams[i] = processor.Process(outputStream);
				}
			}

			for (int i = 0; i < managerAddresses.Length; ++i) {
				IEnumerable<Message> inputStream = inputStreams[i];
				if (inputStream != null) {
					IServiceAddress manager = managerAddresses[i];
					// If any errors happened,
					foreach (Message m in inputStream) {
						// If it's a connection fail message, we try connecting to another
						// manager.
						if (IsConnectionFailMessage(m)) {
							// Tell the tracker it's down,
							serviceTracker.ReportServiceDownClientReport(manager, ServiceType.Manager);
							break;
						}
					}
				}
			}
		}

		public IEnumerable<Message> ProcessManager(IEnumerable<Message> outputStream) {
			// We go through all the manager addresses from first to last until we
			// find one that is currently up,

			// This uses a service status tracker object maintained by the network
			// cache to keep track of manager servers that aren't operational.

			IEnumerable<Message> inputStream = null;
			for (int i = 0; i < managerAddresses.Length; ++i) {
				IServiceAddress manager = managerAddresses[i];
				if (serviceTracker.IsServiceUp(manager, ServiceType.Manager)) {
					IMessageProcessor processor = connector.Connect(manager, ServiceType.Manager);
					inputStream = processor.Process(outputStream);

					bool failed = false;
					// If any errors happened,
					foreach (Message m in inputStream) {
						// If it's a connection fail message, we try connecting to another
						// manager.
						if (IsConnectionFailMessage(m)) {
							// Tell the tracker it's down,
							serviceTracker.ReportServiceDownClientReport(manager, ServiceType.Manager);
							failed = true;
							break;
						}
					}

					if (!failed) {
						return inputStream;
					}
				}
			}

			// If we didn't even try one, we test the first manager.
			if (inputStream == null) {
				IMessageProcessor processor = connector.Connect(managerAddresses[0], ServiceType.Manager);
				inputStream = processor.Process(outputStream);
			}

			// All managers are currently down, so return the last input stream,
			return inputStream;
		}

		private Message ProcessSingleRoot(IEnumerable<Message> outputStream, IServiceAddress rootServer) {
			IMessageProcessor processor = connector.Connect(rootServer, ServiceType.Root);
			IEnumerable<Message> inputStream = processor.Process(outputStream);
			Message lastM = null;
			foreach (Message m in inputStream) {
				lastM = m;
				if (m.HasError) {
					// If it's a connection failure, inform the service tracker and throw
					// service not available exception.
					if (IsConnectionFailMessage(m)) {
						serviceTracker.ReportServiceDownClientReport(rootServer, ServiceType.Root);
						throw new ServiceNotConnectedException(rootServer.ToString());
					}

					string errorSource = m.Error.Source;
					// Rethrow InvalidPathInfoException locally,
					if (errorSource.Equals("Deveel.Data.Net.InvalidPathInfoException")) {
						throw new InvalidPathInfoException(m.ErrorMessage);
					}
				}
			}
			return lastM;
		}

		private PathInfo GetPathInfoFor(string pathName) {
			PathInfo pathInfo = networkCache.GetPathInfo(pathName);
			if (pathInfo == null) {
				// Path info not found in the cache, so query the manager cluster for the
				// info.

				MessageStream outputStream = new MessageStream();
				outputStream.AddMessage(new Message("getPathInfoForPath", pathName));

				IEnumerable<Message> inputStream = ProcessManager(outputStream);

				foreach (Message m in inputStream) {
					if (m.HasError) {
						log.Error(String.Format("'getPathInfoFor' command failed: {0}", m.ErrorMessage));
						log.Error(m.ErrorStackTrace);
						throw new ApplicationException(m.ErrorMessage);
					}

					pathInfo = (PathInfo) m.Arguments[0].Value;
				}

				if (pathInfo == null)
					throw new ApplicationException("Path not found: " + pathName);

				// Put it in the local cache,
				networkCache.SetPathInfo(pathName, pathInfo);
			}
			return pathInfo;
		}

		public DataAddress CreateDatabase() {
			// The child reference is a sparse node element
			NodeId childId = NodeId.CreateSpecialSparseNode((byte) 1, 4);

			// Create a branch,
			TreeBranch rootBranch = new TreeBranch(NodeId.CreateInMemoryNode(0L), MaxBranchSize);
			rootBranch.Set(childId, 4, Key.Tail, childId, 4);

			TreeWrite seq = new TreeWrite();
			seq.NodeWrite(rootBranch);
			IList<NodeId> refs = Persist(seq);

			// The written root node reference,
			NodeId rootId = refs[0];

			// Return the root,
			return new DataAddress(rootId);
		}

		public ITransaction CreateTransaction(DataAddress rootNode) {
			return new NetworkTreeSystemTransaction(this, 0, rootNode);
		}

		public ITransaction CreateTransaction() {
			return new NetworkTreeSystemTransaction(this, 0);
		}

		public DataAddress FlushTransaction(ITransaction transaction) {
			NetworkTreeSystemTransaction netTransaction = (NetworkTreeSystemTransaction) transaction;
			try {
				netTransaction.Checkout();
				return new DataAddress(netTransaction.RootNodeId);
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		public DataAddress PerformCommit(String pathName, DataAddress proposal) {
			// Get the PathInfo object for the given path name,
			PathInfo pathInfo = GetPathInfoFor(pathName);

			// We can only commit on the root leader,
			IServiceAddress rootServer = pathInfo.RootLeader;
			try {
				// TODO; If the root leader is not available, we need to go through
				//   a new leader election process.

				MessageStream outputStream = new MessageStream();
				outputStream.AddMessage(new Message("commit", pathName, pathInfo.VersionNumber, proposal));

				Message m = ProcessSingleRoot(outputStream, rootServer);

				if (m.HasError) {
					// Rethrow commit fault locally,
					if (m.Error.Source.Equals("Deveel.Data.Net.CommitFaultException"))
						throw new CommitFaultException(m.ErrorMessage);

					throw new ApplicationException(m.ErrorMessage);
				}
				// Return the DataAddress of the result transaction,
				return (DataAddress) m.Arguments[0].Value;

			} catch (InvalidPathInfoException) {
				// Clear the cache and requery the manager server for a new path info,
				networkCache.SetPathInfo(pathName, null);
				return PerformCommit(pathName, proposal);
			}
		}

		internal String[] FindAllPaths() {
			MessageStream outputStream = new MessageStream();
			outputStream.AddMessage(new Message("getAllPaths"));

			// Process a command on the manager,
			IEnumerable<Message> inputStream = ProcessManager(outputStream);

			foreach (Message m in inputStream) {
				if (m.HasError) {
					log.Error(String.Format("'getAllPaths' command failed: {0}", m.ErrorMessage));
					log.Error(m.ErrorStackTrace);
					throw new ApplicationException(m.ErrorMessage);
				}

				return (String[]) m.Arguments[0].Value;
			}

			throw new ApplicationException("Bad formatted message stream");
		}

		internal string GetPathType(string pathName) {
			PathInfo pathInfo = GetPathInfoFor(pathName);
			return pathInfo.PathType;
		}

		private DataAddress InternalGetPathNow(PathInfo pathInfo, IServiceAddress rootServer) {
			MessageStream outputStream = new MessageStream();
			outputStream.AddMessage(new Message("getPathNow", pathInfo.PathName, pathInfo.VersionNumber));

			Message m = ProcessSingleRoot(outputStream, rootServer);
			if (m.HasError)
				throw new ApplicationException(m.ErrorMessage);

			return (DataAddress) m.Arguments[0].Value;
		}

		internal DataAddress GetPathNow(string pathName) {
			// Get the PathInfo object for the given path name,
			PathInfo pathInfo = GetPathInfoFor(pathName);

			// Try the root leader first,
			IServiceAddress rootServer = pathInfo.RootLeader;
			try {
				DataAddress dataAddress = InternalGetPathNow(pathInfo, rootServer);

				// TODO: if the root leader is not available, query the replicated
				//   root servers with this path.

				return dataAddress;
			} catch (InvalidPathInfoException e) {
				// Clear the cache and requery the manager server for a new path info,
				networkCache.SetPathInfo(pathName, null);
				return GetPathNow(pathName);
			}
		}

		private DataAddress[] InternalGetPathHistorical(PathInfo pathInfo, IServiceAddress server,
			long timeStart, long timeEnd) {
			Message message = new Message("getPathHistorical", pathInfo.PathName, pathInfo.VersionNumber, timeStart, timeEnd);

			Message m = ProcessSingleRoot(message.AsStream(), server);
			if (m.HasError)
				throw new ApplicationException(m.ErrorMessage);

			return (DataAddress[]) m.Arguments[0].Value;
		}

		internal DataAddress[] GetPathHistorical(string pathName, long timeStart, long timeEnd) {

			// Get the PathInfo object for the given path name,
			PathInfo pathInfo = GetPathInfoFor(pathName);

			// Try the root leader first,
			IServiceAddress rootServer = pathInfo.RootLeader;
			try {
				DataAddress[] dataAddresses = InternalGetPathHistorical(pathInfo, rootServer, timeStart, timeEnd);

				// TODO: if the root leader is not available, query the replicated
				//   root servers with this path.

				return dataAddresses;

			} catch (InvalidPathInfoException) {
				// Clear the cache and requery the manager server for a new path info,
				networkCache.SetPathInfo(pathName, null);
				return GetPathHistorical(pathName, timeStart, timeEnd);
			}
		}

		internal void DisposeTransaction(ITransaction transaction) {
			((NetworkTreeSystemTransaction) transaction).Dispose();
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
				foreach (NodeId nodeId in nids) {
					if (nodeId.IsSpecial) {
						resultNodes[i] = nodeId.CreateSpecialTreeNode();
					}
					++i;
				}
			}

			// Group all the nodes to the same block,
			List<BlockId> uniqueBlocks = new List<BlockId>();
			List<List<NodeId>> uniqueBlockList = new List<List<NodeId>>();
			{
				int i = 0;
				foreach (NodeId nodeId in nids) {
					// If it's not a special node,
					if (!nodeId.IsSpecial) {
						// Get the block id and add it to the list of unique blocks,
						DataAddress address = new DataAddress(nodeId);
						// Check if the node is in the local cache,
						ITreeNode node = networkCache.GetNode(address);
						if (node != null) {
							resultNodes[i] = node;
						} else {
							// Not in the local cache so we need to bundle this up in a node
							// request on the block servers,
							// Group this node request by the block identifier
							BlockId blockId = address.BlockId;
							int ind = uniqueBlocks.IndexOf(blockId);
							if (ind == -1) {
								ind = uniqueBlocks.Count;
								uniqueBlocks.Add(blockId);
								uniqueBlockList.Add(new List<NodeId>());
							}
							List<NodeId> blist = uniqueBlockList[ind];
							blist.Add(nodeId);
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
			IDictionary<BlockId, IList<BlockServerElement>> serversMap = GetServerListForBlocks(uniqueBlocks);

			// The result nodes list,
			List<ITreeNode> nodes = new List<ITreeNode>();

			// Checksumming objects
			byte[] checksumBuf = null;
			Crc32 crc32 = null;

			// For each unique block list,
			foreach (List<NodeId> blist in uniqueBlockList) {
				// Make a block server request for each node in the block,
				MessageStream blockServerMsg = new MessageStream();
				BlockId blockId = null;
				foreach (NodeId nodeId in blist) {
					DataAddress address = new DataAddress(nodeId);
					blockServerMsg.AddMessage(new Message("readFromBlock", address));
					blockId = address.BlockId;
				}

				if (blockId == null) {
					throw new ApplicationException("block_id == null");
				}

				// Get the shuffled list of servers the block is stored on,
				IList<BlockServerElement> servers = serversMap[blockId];

				// Go through the servers one at a time to fetch the block,
				bool success = false;
				for (int z = 0; z < servers.Count && !success; ++z) {
					BlockServerElement server = servers[z];
					// If the server is up,
					if (server.IsStatusUp) {
						// Open a connection with the block server,
						IMessageProcessor blockServerProc = connector.Connect(server.Address, ServiceType.Block);
						IEnumerable<Message> messageIn = blockServerProc.Process(blockServerMsg);
						++NetworkCommCount;
						++NetworkFetchCommCount;

						bool isError = false;
						bool severeError = false;
						bool crcError = false;
						bool connectionError = false;

						// Turn each none-error message into a node
						foreach (Message m in messageIn) {
							if (m.HasError) {
								// See if this error is a block read error. If it is, we don't
								// tell the manager server to lock this server out completely.
								bool isBlockReadError = m.Error.Source.Equals("Deveel.Data.Net.BlockReadException");
								// If it's a connection fault,
								if (IsConnectionFailMessage(m)) {
									connectionError = true;
								} else if (!isBlockReadError) {
									// If it's something other than a block read error or
									// connection failure, we set the severe flag,
									severeError = true;
								}
								isError = true;
							} else if (isError == false) {
								// The reply contains the block of data read.
								NodeSet nodeSet = (NodeSet) m.Arguments[0].Value;

								DataAddress address = null;

								// Catch any IOExceptions (corrupt zips, etc)
								try {
									// Decode the node items into Java node objects,
									foreach (Node nodeItem in nodeSet) {
										NodeId nodeId = nodeItem.Id;

										address = new DataAddress(nodeId);
										// Wrap around a buffered DataInputStream for reading values
										// from the store.
										BinaryReader input = new BinaryReader(nodeItem.Input);
										short nodeType = input.ReadInt16();

										ITreeNode readNode = null;

										if (crc32 == null)
											crc32 = new Crc32();
										crc32.Initialize();

										// Is the node type a leaf node?
										if (nodeType == StoreLeafType) {
											// Read the checksum,
											input.ReadInt16(); // For future use...
											int checksum = input.ReadInt32();
											// Read the size
											int leafSize = input.ReadInt32();

											byte[] buf = StreamUtil.AsBuffer(nodeItem.Input);
											if (buf == null) {
												buf = new byte[leafSize + 12];
												ByteBuffer.WriteInt4(leafSize, buf, 8);
												input.Read(buf, 12, leafSize);
											}

											// Check the checksum...
											crc32.ComputeHash(buf, 8, leafSize + 4);
											int calcChecksum = (int) crc32.CrcValue;
											if (checksum != calcChecksum) {
												// If there's a CRC failure, we reject his node,
												log.Warning(String.Format("CRC failure on node {0} @ {1}", nodeId, server.Address));
												isError = true;
												crcError = true;
												// This causes the read to retry on a different server
												// with this block id
											} else {
												// Create a leaf that's mapped to this data
												ITreeNode leaf = new MemoryTreeLeaf(nodeId, buf);
												readNode = leaf;
											}

										}
											// Is the node type a branch node?
										else if (nodeType == StoreBranchType) {
											// Read the checksum,
											input.ReadInt16(); // For future use...
											int checksum = input.ReadInt32();

											// Check the checksum objects,
											if (checksumBuf == null)
												checksumBuf = new byte[8];

											// Note that the entire branch is loaded into memory,
											int childDataSize = input.ReadInt32();
											ByteBuffer.WriteInt4(childDataSize, checksumBuf, 0);
											crc32.ComputeHash(checksumBuf, 0, 4);
											long[] dataArr = new long[childDataSize];
											for (int n = 0; n < childDataSize; ++n) {
												long item = input.ReadInt64();
												ByteBuffer.WriteInt8(item, checksumBuf, 0);
												crc32.ComputeHash(checksumBuf, 0, 8);
												dataArr[n] = item;
											}

											// The calculated checksum value,
											int calcChecksum = (int) crc32.CrcValue;
											if (checksum != calcChecksum) {
												// If there's a CRC failure, we reject his node,
												log.Warning(String.Format("CRC failure on node {0} @ {1}", nodeId, server.Address));
												isError = true;
												crcError = true;
												// This causes the read to retry on a different server
												// with this block id
											} else {
												// Create the branch node,
												TreeBranch branch =
													new TreeBranch(nodeId, dataArr, childDataSize);
												readNode = branch;
											}

										} else {
											log.Error(String.Format("Unknown node {0} type: {1}", address, nodeType));
											isError = true;
										}

										// Is the node already in the list? If so we don't add it.
										if (readNode != null && !IsInNodeList(nodeId, nodes)) {
											// Put the read node in the cache and add it to the 'nodes'
											// list.
											networkCache.SetNode(address, readNode);
											nodes.Add(readNode);
										}

									} // while (item_iterator.hasNext())

								} catch (IOException e) {
									// This catches compression errors, as well as any other misc
									// IO errors.
									if (address != null) {
										log.Error(String.Format("IO Error reading node {0}", address));
									}
									log.Error(e.Message, e);
									isError = true;
								}

							}

						} // for (Message m : message_in)

						// If there was no error while reading the result, we assume the node
						// requests were successfully read.
						if (isError == false) {
							success = true;
						} else {
							// If this is a connection failure, we report the block failure.
							if (connectionError) {
								// If this is an error, we need to report the failure to the
								// manager server,
								ReportBlockServerFailure(server.Address);
								// Remove the block id from the server list cache,
								networkCache.RemoveServersWithBlock(blockId);
							} else {
								String failType = "General";
								if (crcError) {
									failType = "CRC Failure";
								} else if (severeError) {
									failType = "Exception during process";
								}

								// Report to the first manager the block failure, so it may
								// investigate and hopefully correct.
								ReportBlockIdCorruption(server.Address, blockId, failType);

								// Otherwise, not a severe error (probably a corrupt block on a
								// server), so shuffle the server list for this block_id so next
								// time there's less chance of hitting this bad block.
								IEnumerable<BlockServerElement> srvs = networkCache.GetServersWithBlock(blockId);
								if (srvs != null) {
									List<BlockServerElement> serverList = new List<BlockServerElement>();
									serverList.AddRange(srvs);
									CollectionsUtil.Shuffle(serverList);
									networkCache.SetServersForBlock(blockId, serverList, 15*60*1000);
								}
							}
							// We will now go retry the query on the next block server,
						}

					}
				}

				// If the nodes were not successfully read, we generate an exception,
				if (!success) {
					// Remove from the cache,
					networkCache.RemoveServersWithBlock(blockId);
					throw new ApplicationException(
						"Unable to fetch node from a block server" +
						" (block = " + blockId + ")");
				}
			}

			int sz = nodes.Count;
			if (sz == 0) {
				throw new ApplicationException("Empty nodes list");
			}

			for (int i = 0; i < sz; ++i) {
				ITreeNode node = nodes[i];
				NodeId nodeId = node.Id;
				for (int n = 0; n < nids.Length; ++n) {
					if (nids[n].Equals(nodeId)) {
						resultNodes[n] = node;
					}
				}
			}

			// Check the result_nodes list is completely populated,
			for (int n = 0; n < resultNodes.Length; ++n) {
				if (resultNodes[n] == null) {
					throw new ApplicationException("Assertion failed: result_nodes not completely populated.");
				}
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

		private bool IsInNodeList(NodeId id, IEnumerable<ITreeNode> nodes) {
			foreach (ITreeNode node in nodes) {
				if (id.Equals(node.Id))
					return true;
			}
			return false;
		}

		private NodeId[] InternalPersist(TreeWrite sequence, int tryCount) {
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
			NodeId[] outNodeIds = new NodeId[sz];

			MessageStream allocateMessageStream = new MessageStream();

			// Allocate the space first,
			for (int i = 0; i < sz; ++i) {
				ITreeNode node = nodes[i];
				// Is it a branch node?
				if (node is TreeBranch) {
					// Branch nodes are 1K in size,
					allocateMessageStream.AddMessage(new Message("allocateNode", 1024));
				}
					// Otherwise, it must be a leaf node,
				else {
					// Leaf nodes are 4k in size,
					allocateMessageStream.AddMessage(new Message("allocateNode", 4096));
				}
			}

			// Process a command on the manager,
			IEnumerable<Message> resultStream = ProcessManager(allocateMessageStream);

			// The unique list of blocks,
			List<BlockId> uniqueBlocks = new List<BlockId>();

			// Parse the result stream one message at a time, the order will be the
			// order of the allocation messages,
			int n = 0;
			foreach (Message m in resultStream) {
				if (m.HasError)
					throw new ApplicationException(m.ErrorMessage);

				DataAddress addr = (DataAddress) m.Arguments[0].Value;
				refs[n] = addr;
				// Make a list of unique block identifiers,
				if (!uniqueBlocks.Contains(addr.BlockId)) {
					uniqueBlocks.Add(addr.BlockId);
				}
				++n;
			}

			// Get the block to server map for each of the blocks,

			IDictionary<BlockId, IList<BlockServerElement>> blockToServerMap =
				GetServerListForBlocks(uniqueBlocks);

			// Make message streams for each unique block
			int ubidCount = uniqueBlocks.Count;
			MessageStream[] ubidStream = new MessageStream[ubidCount];
			for (int i = 0; i < ubidStream.Length; ++i) {
				ubidStream[i] = new MessageStream();
			}

			// Scan all the blocks and create the message streams,
			for (int i = 0; i < sz; ++i) {

				byte[] nodeBuf;

				ITreeNode node = nodes[i];
				// Is it a branch node?
				if (node is TreeBranch) {
					TreeBranch branch = (TreeBranch) node;
					// Make a copy of the branch (NOTE; we clone() the array here).
					long[] curNodeData = (long[]) branch.NodeData.Clone();
					int curNdsz = branch.NodeDataSize;
					branch = new TreeBranch(refs[i].Value, curNodeData, curNdsz);

					// The number of children
					int chsz = branch.ChildCount;
					// For each child, if it's a heap node, look up the child id and
					// reference map in the sequence and set the reference accordingly,
					for (int o = 0; o < chsz; ++o) {
						NodeId childId = branch.GetChild(o);
						if (childId.IsInMemory) {
							// The ref is currently on the heap, so adjust accordingly
							int refId = sequence.LookupRef(i, o);
							branch.SetChildOverride(refs[refId].Value, o);
						}
					}

					// Turn the branch into a 'node_buf' byte[] array object for
					// serialization.
					long[] nodeData = branch.NodeData;
					int ndsz = branch.NodeDataSize;
					MemoryStream bout = new MemoryStream(1024);
					BinaryWriter dout = new BinaryWriter(bout);
					dout.Write(StoreBranchType);
					dout.Write((short) 0); // Reserved for future
					dout.Write(0); // The crc32 checksum will be written here,
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
					ByteBuffer.WriteInt4((int) checksum.CrcValue, nodeBuf, 4);

					// Put this branch into the local cache,
					networkCache.SetNode(refs[i], branch);

				}
					// If it's a leaf node,
				else {
					TreeLeaf leaf = (TreeLeaf) node;
					int lfsz = leaf.Length;

					nodeBuf = new byte[lfsz + 12];

					// Format the data,
					ByteBuffer.WriteInt2(StoreLeafType, nodeBuf, 0);
					ByteBuffer.WriteInt2(0, nodeBuf, 2); // Reserved for future
					ByteBuffer.WriteInt4(lfsz, nodeBuf, 8);
					leaf.Read(0, nodeBuf, 12, lfsz);

					// Calculate and set the checksum,
					Crc32 checksum = new Crc32();
					checksum.ComputeHash(nodeBuf, 8, nodeBuf.Length - 8);
					ByteBuffer.WriteInt4((int) checksum.CrcValue, nodeBuf, 4);

					// Put this leaf into the local cache,
					leaf = new MemoryTreeLeaf(refs[i].Value, nodeBuf);
					networkCache.SetNode(refs[i], leaf);

				}

				// The DataAddress this node is being written to,
				DataAddress address = refs[i];
				// Get the block id,
				BlockId blockId = address.BlockId;
				int bid = uniqueBlocks.IndexOf(blockId);
				ubidStream[bid].AddMessage(new Message("writeToBlock", address, nodeBuf, 0, nodeBuf.Length));

				// Update 'out_refs' array,
				outNodeIds[i] = refs[i].Value;
			}

			// A log of successfully processed operations,
			List<object> successProcess = new List<object>(64);

			// Now process the streams on the servers,
			for (int i = 0; i < ubidStream.Length; ++i) {
				// The output message,
				MessageStream outputStream = ubidStream[i];
				// Get the servers this message needs to be sent to,
				BlockId blockId = uniqueBlocks[i];
				IList<BlockServerElement> blockServers = blockToServerMap[blockId];
				// Format a message for writing this node out,
				int bssz = blockServers.Count;
				IMessageProcessor[] blockServerProcs = new IMessageProcessor[bssz];
				// Make the block server connections,
				for (int o = 0; o < bssz; ++o) {
					IServiceAddress address = blockServers[o].Address;
					blockServerProcs[o] = connector.Connect(address, ServiceType.Block);
					IEnumerable<Message> inputStream = blockServerProcs[o].Process(outputStream);
					++NetworkCommCount;

					foreach (Message m in inputStream) {
						if (m.HasError) {
							// If this is an error, we need to report the failure to the
							// manager server,
							ReportBlockServerFailure(address);
							// Remove the block id from the server list cache,
							networkCache.RemoveServersWithBlock(blockId);

							// Rollback any server writes already successfully made,
							for (int p = 0; p < successProcess.Count; p += 2) {
								IServiceAddress blocksAddr = (IServiceAddress) successProcess[p];
								MessageStream toRollback = (MessageStream) successProcess[p + 1];

								List<DataAddress> rollbackNodes = new List<DataAddress>(128);
								foreach (Message rm in toRollback) {
									DataAddress raddr = (DataAddress) rm.Arguments[0].Value;
									rollbackNodes.Add(raddr);
								}
								// Create the rollback message,
								MessageStream rollbackMsg = new MessageStream();
								rollbackMsg.AddMessage(new Message("rollbackNodes", new object[] {rollbackNodes.ToArray()}));

								// Send it to the block server,
								IEnumerable<Message> responseStream = connector.Connect(blocksAddr, ServiceType.Block).Process(rollbackMsg);
								++NetworkCommCount;
								foreach (Message rbm in responseStream) {
									// If rollback generated an error we throw the error now
									// because this likely is a serious network error.
									if (rbm.HasError) {
										throw new NetworkWriteException("Write failed (rollback failed): " + rbm.ErrorMessage);
									}
								}

							}

							// Retry,
							if (tryCount > 0)
								return InternalPersist(sequence, tryCount - 1);

							// Otherwise we fail the write
							throw new NetworkWriteException(m.ErrorMessage);
						}
					}

					// If we succeeded without an error, add to the log
					successProcess.Add(address);
					successProcess.Add(outputStream);

				}
			}

			// Return the references,
			return outNodeIds;
		}


		public bool LinkLeaf(Key key, NodeId id) {
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
			log.Error("Error state", error);
			errorState = new ErrorStateException(error.Message, error);
			throw errorState;
		}

		public void CheckErrorState() {
			// We wrap the critical stop error a second time to ensure the stack
			// trace accurately reflects where the failure originated.

			if (errorState != null)
				throw new ErrorStateException(errorState.Message, errorState);
		}

		private void ReportBlockServerFailure(IServiceAddress address) {
			// Report the failure,
			log.Warning(String.Format("Reporting failure for {0} to manager server", address));

			// Failure throttling,
			lock (failureFloodControl) {
				DateTime currentTime = DateTime.Now;
				DateTime lastAddressFailTime;
				if (failureFloodControl.TryGetValue(address, out lastAddressFailTime) &&
				    lastAddressFailTime.AddMilliseconds((30*1000)) > currentTime) {
					// We don't respond to failure notifications on the same address if a
					// failure notice arrived within a minute of the last one accepted.
					return;
				}
				failureFloodControl[address] = currentTime;
			}

			Message message = new Message("notifyBlockServerFailure", address);

			// Process the failure report message on the manager server,
			NotifyAllManagers(message.AsStream());
		}

		private void ReportBlockIdCorruption(IServiceAddress blockServer, BlockId blockId, String failType) {
			// Report the failure,
			log.Warning(String.Format("Reporting a data failure (type = {0}) for block {1} at block server {2}",
			                          failType, blockId, blockServer));

			// Failure throttling,
			lock (failureFloodControlBidc) {
				DateTime currentTime = DateTime.Now;
				DateTime lastAddressFailTime;
				if (failureFloodControlBidc.TryGetValue(blockServer, out lastAddressFailTime) &&
				    lastAddressFailTime.AddMilliseconds((10*1000)) > currentTime) {
					// We don't respond to failure notifications on the same address if a
					// failure notice arrived within a minute of the last one accepted.
					return;
				}

				failureFloodControlBidc[blockServer] = currentTime;
			}

			Message message = new Message("notifyBlockIdCorruption", blockServer, blockId, failType);

			// Process the failure report message on the manager server,
			// (Ignore any error message generated)
			ProcessManager(message.AsStream());
		}

		private IDictionary<BlockId, IList<BlockServerElement>> GetServerListForBlocks(List<BlockId> blockIds) {
			// The result map,
			Dictionary<BlockId, IList<BlockServerElement>> resultMap = new Dictionary<BlockId, IList<BlockServerElement>>();

			List<BlockId> noneCached = new List<BlockId>(blockIds.Count);
			foreach (BlockId blockId in blockIds) {
				IList<BlockServerElement> v = networkCache.GetServersWithBlock(blockId);
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
			if (noneCached.Count == 0) {
				// Return the result,
				return resultMap;
			}

			// Otherwise, we query the manager server for current records on the given
			// blocks.

			MessageStream outputStream = new MessageStream();

			foreach (BlockId blockId in noneCached) {
				outputStream.AddMessage(new Message("getServerList", blockId));
			}

			// Process a command on the manager,
			IEnumerable<Message> inputStream = ProcessManager(outputStream);

			int n = 0;
			foreach (Message m in inputStream) {
				if (m.HasError)
					throw new ApplicationException(m.ErrorMessage);

				int sz = (int) m.Arguments[0].Value;
				List<BlockServerElement> srvs = new List<BlockServerElement>(sz);
				for (int i = 0; i < sz; ++i) {
					IServiceAddress address = (IServiceAddress) m.Arguments[1 + (i*2)].Value;
					ServiceStatus status = (ServiceStatus) m.Arguments[1 + (i*2) + 1].Value;
					srvs.Add(new BlockServerElement(address, status));
				}

				// Shuffle the list
				CollectionsUtil.Shuffle(srvs);

				// Move the server closest to this node to the start of the list,
				int closest = 0;
				int curCloseFactor = Int32.MaxValue;
				for (int i = 0; i < sz; ++i) {
					BlockServerElement elem = srvs[i];
					int closenessFactor = FindClosenessToHere(elem.Address);
					if (closenessFactor < curCloseFactor) {
						curCloseFactor = closenessFactor;
						closest = i;
					}
				}

				// Swap if necessary,
				if (closest > 0)
					CollectionsUtil.Swap(srvs, 0, closest);

				// Put it in the result map,
				BlockId blockId = noneCached[n];
				resultMap[blockId] = srvs;
				// Add it to the cache,
				// NOTE: TTL hard-coded to 15 minute
				networkCache.SetServersForBlock(blockId, srvs, 15*60*1000);
				++n;
			}

			// Return the list
			return resultMap;
		}

		private int FindClosenessToHere(IServiceAddress address) {
			TcpServiceAddress tcpAddress = address as TcpServiceAddress;
			if (tcpAddress == null)
				return Int32.MaxValue;

			lock(closenessMap) {
				int closeness;
				if (!closenessMap.TryGetValue(address, out closeness)) {

					try {
						IPAddress machineAddress = tcpAddress.ToIPAddress();

						IEnumerable<NetworkInterface> localInterfaces = NetworkInterface.GetAllNetworkInterfaces();
						bool isLocal = false;
						
						foreach (NetworkInterface netint in localInterfaces)
						{
							IEnumerable<IPAddress> addresses = netint.GetIPProperties().DnsAddresses;
							foreach (IPAddress addr in addresses)
							{
								// If this machine address is on this machine, return true,
								if (machineAddress.Equals(addr)) {
									isLocal = true;
									goto interface_loop;
								}
							}
						}

						interface_loop:;
						// If the interface is local,
						if (isLocal) {
							closeness = 0;
						} else {
							// Not local,
							closeness = 10000;
						}

					} catch (SocketException e) {
						// Unknown closeness,
						// Log a severe error,
						log.Error("Unable to determine if node local", e);
						closeness = Int32.MaxValue;
					}

					// Put it in the map,
					closenessMap[address] = closeness;
				}

				return closeness;
			}
		}


		public IList<NodeId> Persist(TreeWrite write) {
			return InternalPersist(write, 3);
		}

		private void doReachCheck(TextWriter warningLog, NodeId node, SortedIndex nodeList, int curDepth) {
			throw new NotImplementedException();
		}

		public void CreateReachabilityList(TextWriter warningLog, NodeId node, SortedIndex nodeList) {
			CheckErrorState();
			lock (reachabilityLock) {
				reachabilityTreeDepth = -1;
				doReachCheck(warningLog, node, nodeList, 1);
			}
		}

		public TreeReportNode CreateDiagnosticGraph(ITransaction t) {
			CheckErrorState();

			// The key object transaction
			NetworkTreeSystemTransaction ts = (NetworkTreeSystemTransaction) t;
			// Get the root node ref
			NodeId rootNodeId = ts.RootNodeId;
			// Add the child node (the root node of the version graph).
			return CreateDiagnosticRootGraph(Key.Head, rootNodeId);
		}

		private TreeReportNode CreateDiagnosticRootGraph(Key leftKey, NodeId id) {
			// The node being returned
			TreeReportNode node;

			// Fetch the node,
			ITreeNode treeNode = FetchNodes(new NodeId[] {id})[0];

			if (treeNode is TreeLeaf) {
				TreeLeaf leaf = (TreeLeaf) treeNode;
				// The number of bytes in the leaf
				int leafSize = leaf.Length;

				// Set up the leaf node object
				node = new TreeReportNode("leaf", id);
				node.SetProperty("key", leftKey.ToString());
				node.SetProperty("leaf_size", leafSize);

			} else if (treeNode is TreeBranch) {
				TreeBranch branch = (TreeBranch) treeNode;
				// Set up the branch node object
				node = new TreeReportNode("branch", id);
				node.SetProperty("key", leftKey.ToString());
				node.SetProperty("branch_size", branch.ChildCount);
				// Recursively add each child into the tree
				for (int i = 0; i < branch.ChildCount; ++i) {
					NodeId childId = branch.GetChild(i);
					// If the ref is a special node, skip it
					if (childId.IsSpecial) {
						// Should we record special nodes?
					} else {
						Key newLeftKey = (i > 0) ? branch.GetKey(i) : leftKey;
						TreeReportNode bn = new TreeReportNode("child_meta", id);
						bn.SetProperty("extent", branch.GetChildLeafElementCount(i));
						node.ChildNodes.Add(bn);
						node.ChildNodes.Add(CreateDiagnosticRootGraph(newLeftKey, childId));
					}
				}

			} else {
				throw new IOException("Unknown node class: " + treeNode);
			}

			return node;
		}

		#region MemoryTreeLeaf

		class MemoryTreeLeaf : TreeLeaf {
			private readonly byte[] data;
			private readonly NodeId nodeId;

			public MemoryTreeLeaf(NodeId nodeId, byte[] data) {
				this.nodeId = nodeId;
				this.data = data;
			}


			public override int Length {
				get { return data.Length - 12; }
			}

			public override int Capacity {
				get { throw new ApplicationException("Immutable leaf does not have a meaningful capacity"); }
			}

			public override NodeId Id {
				get { return nodeId; }
			}

			public override long MemoryAmount {
				get { return 8 + data.Length + 64; }
			}

			public override void SetLength(int value) {
				throw new IOException("Write methods not available for immutable leaf");
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				Array.Copy(data, 12 + position, buffer, offset, count);
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				throw new IOException("Write methods not available for immutable leaf");
			}

			public override void WriteTo(IAreaWriter area) {
				area.Write(data, 12, Length);
			}

			public override void Shift(int position, int offset) {
				throw new IOException("Write methods not available for immutable leaf");
			}
		}

		#endregion

		#region NetworkTreeSystemTransaction

		class NetworkTreeSystemTransaction : TreeSystemTransaction {
			public NetworkTreeSystemTransaction(ITreeSystem treeStore, long versionId) 
				: this(treeStore, versionId, null) {
				SetToEmpty();
			}

			public NetworkTreeSystemTransaction(ITreeSystem treeStore, long versionId, DataAddress rootNodeId) 
				: base(treeStore, versionId, rootNodeId.Value, false) {
			}
		}

		#endregion
	}
}