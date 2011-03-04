using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public abstract class BlockService : Service {
		private long serverGuid;
		private volatile int blockCount;
		private readonly Dictionary<int, BlockId> maxKnownBlockId = new Dictionary<int, BlockId>(16);
		private readonly IServiceConnector connector;
		
		private readonly Dictionary<BlockId, BlockContainer> blockContainerCache;
		private readonly LinkedList<BlockContainer> blockContainerAccessList;
		private readonly LinkedList<BlockContainer> blocksPendingFlush;
		private Timer blockFlushTimer;
		private readonly object pathLock = new object();
		
		private readonly object processLock = new object();
		private readonly object blockUploadLock = new object();
		private int processId;
		private Timer sendBlockTimer;
		
		protected BlockService(IServiceConnector connector) {
			this.connector = connector;
			
			blockContainerCache = new Dictionary<BlockId, BlockContainer>(5279);
			blockContainerAccessList = new LinkedList<BlockContainer>();
			blocksPendingFlush = new LinkedList<BlockContainer>();
		}
		
		public override ServiceType ServiceType {
			get { return ServiceType.Block; }
		}
		
		public int BlockCount {
			get { return blockCount; }
		}

		protected object BlockUploadSyncRoot {
			get { return blockUploadLock; }
		}
				
		private BlockContainer FetchBlockContainer(BlockId blockId) {
			// Check for stop state,
			CheckErrorState();

			BlockContainer container;

			lock (pathLock) {
				// Look up the block container in the map,
				// If the container not in the map, create it and put it in there,
				if (!blockContainerCache.TryGetValue(blockId, out container)) {
					container = LoadBlock(blockId);
					// Put it in the map,
					blockContainerCache[blockId] = container;
				}

				// We manage a small list of containers that have been accessed ordered
				// by last access time.

				// Iterate through the list. If we discover the BlockContainer recently
				// accessed we move it to the front.
				LinkedListNode<BlockContainer> node = blockContainerAccessList.First;
				while (node != null) {
					BlockContainer bc = node.Value;
					if (bc == container) {
						// Found, so move it to the front,
						blockContainerAccessList.Remove(bc);
						blockContainerAccessList.AddFirst(container);
						// Return the container,
						return container;
					}
					node = node.Next;
				}

				// The container isn't found on the access list, so we need to add
				// it.

				// If the size of the list is over some threshold, we clear out the
				// oldest entry and close it,
				int listSize = blockContainerAccessList.Count;
				if (listSize > 32) {
					blockContainerAccessList.Last.Value.Close();
					blockContainerAccessList.RemoveLast();
				}

				// Add to the list,
				container.Open();
				blockContainerAccessList.AddFirst(container);

				// And return the container,
				return container;
			}
		}
		
		private void ScheduleBlockFlush(BlockContainer container, int delay) {
			lock (pathLock) {
				if (!blocksPendingFlush.Contains(container)) {
					blocksPendingFlush.AddFirst(container);
					blockFlushTimer = new Timer(FlushBlock, container, delay, delay);
				}
			}
		}
		
		private void FlushBlock(object state) {
			BlockContainer container = (BlockContainer)state;
			lock (pathLock) {
				blocksPendingFlush.Remove(container);
				try {
					container.Flush();
				} catch (IOException e) {
					// We log the warning, but otherwise ignore any IO Error on a
					// file synchronize.
					Logger.Warning("Error while flushing block '" + container.BlockId + "': " + e.Message);
				}
			}
		}
		
		private void WriteBlockPart(BlockId blockId, long pos, int storeType, byte[] buffer, int length) {
			// Make sure this process is exclusive
			lock (blockUploadLock) {
				try {
					OnWriteBlockPart(blockId, pos, storeType, buffer, length);
				} catch (IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}

		}
		
		private void CompleteBlockWrite(BlockId blockId, int storeType) {
			// Make sure this process is exclusive
			lock (blockUploadLock) {
				OnCompleteBlockWrite(blockId, storeType);
			}

			// Update internal state as appropriate,
			lock (pathLock) {
				BlockContainer container = LoadBlock(blockId);
				blockContainerCache[blockId] = container;
				++blockCount;
			}

		}
		
		private void CloseContainers() {
			lock(pathLock) {
				foreach(KeyValuePair<BlockId, BlockContainer> pair in blockContainerCache) {
					pair.Value.BlockStore.Close();
				}
			}
		}

		private void NotifyCurrentBlockId(BlockId block_id) {
			// Extract the low byte out of the block_id, which is a key for the manager
			// server that generated this block chain.
			int manager_key = ((int) block_id.Low & 0x0FF);

			// Update the map for this key,
			lock (maxKnownBlockId) {
				maxKnownBlockId[manager_key] = block_id;
			}
		}

		protected bool IsKnownStaticBlock(BlockContainer block) {
			// If the block was written to less than 3 minutes ago, return false
			if (block.LastWriteTime > DateTime.Now.AddMinutes(-3)) {
				return false;
			}
			// Extract the server key from the block id
			BlockId block_id = block.blockId;
			int server_id = ((int) block_id.Low & 0x0FF);

			// Look up the max known block id for the manager server,
			BlockId max_block_id;
			lock (maxKnownBlockId) {
				max_block_id = maxKnownBlockId[server_id];
			}

			// If the block is less than the max, the block can be compressed!
			if (max_block_id != null && block_id.CompareTo(max_block_id) < 0)
				return true;

			// Otherwise update the last write flag (so we only check the max block id
			// every 3 mins).
			block.TouchLastWrite();
			return false;
		}

		protected override void OnStop() {
			lock (pathLock) {
				CloseContainers();
				blockContainerCache.Clear();
				blocksPendingFlush.Clear();
				blockContainerAccessList.Clear();
			}

			if (sendBlockTimer != null)
				sendBlockTimer.Dispose();
			if (blockFlushTimer != null)
				blockFlushTimer.Dispose();

			blockCount = 0;
		}

		protected void SetGuid(long value) {
			serverGuid = value;
		}
						
		protected virtual void OnCompleteBlockWrite(BlockId blockId, int storeType) {
		}
		
		protected virtual void OnWriteBlockPart(BlockId blockId, long pos, int storeType, byte[] buffer, int length) {
			
		}

		protected virtual byte[] CreateAvailabilityMap(BlockId[] blocks) {
			return new byte[0];
		}
		
		protected abstract BlockContainer LoadBlock(BlockId blockId);
		
		protected BlockContainer GetBlock(BlockId blockId) {
			CheckErrorState();
			
			BlockContainer container;
			lock(pathLock) {
				if (!blockContainerCache.TryGetValue(blockId, out container))
					return null;
			}
			
			return container;
		}
		
		protected abstract BlockId[] ListBlocks();

		protected override IMessageProcessor CreateProcessor() {
			return new BlockServerMessageProcessor(this);
		}

		protected override void OnStart() {
			// Read in all the blocks in and populate the map,
			BlockId[] blocks = ListBlocks();

			lock (pathLock) {
				foreach (BlockId blockId in blocks) {
					BlockContainer container = LoadBlock(blockId);
					blockContainerCache[blockId] = container;
				}
			}

			// Discover the block count,
			blockCount = blocks.Length;
		}
		
		#region BlockServerMessageProcessor

		private sealed class BlockServerMessageProcessor : IMessageProcessor {
			private readonly BlockService service;

			public BlockServerMessageProcessor(BlockService service) {
				this.service = service;
			}

			private void CloseContainers(Dictionary<BlockId, BlockContainer> touched) {
				foreach (KeyValuePair<BlockId, BlockContainer> c in touched) {
					service.Logger.Info("Closing block '" + c.Key + "'.");
					c.Value.Close();
				}
			}

			private BlockContainer GetBlock(IDictionary<BlockId, BlockContainer> touched, BlockId blockId) {
				BlockContainer b;
				if (!touched.TryGetValue(blockId, out b)) {
					b = service.FetchBlockContainer(blockId);
					bool created = b.Open();
					if (created) {
						++service.blockCount;
					}
					touched[blockId] = b;
					service.Logger.Info("Opening block '" + blockId + "'.");
				}
				return b;
			}

			private void WriteToBlock(Dictionary<BlockId, BlockContainer> touched, DataAddress address, byte[] buf, int off, int len) {
				// The block being written to,
				BlockId blockId = address.BlockId;
				// The data identifier,
				int dataId = address.DataId;

				// Fetch the block container,
				BlockContainer container = GetBlock(touched, blockId);
				// Write the data,
				container.Write(dataId, buf, off, len);

				// Schedule the block to flushed 5 seconds after a write
				service.ScheduleBlockFlush(container, 5000);
			}

			private NodeSet ReadFromBlock(IDictionary<BlockId, BlockContainer> touched, DataAddress address) {
				// The block being written to,
				BlockId blockId = address.BlockId;
				// The data identifier,
				int dataId = address.DataId;

				// Fetch the block container,
				BlockContainer container = GetBlock(touched, blockId);
				// Read the data,
				return container.GetNodeSet(dataId);
			}

			private void DeleteNodes(IDictionary<BlockId, BlockContainer> touched, DataAddress[] addresses) {
				foreach (DataAddress address in addresses) {
					// The block being removed from,
					BlockId blockId = address.BlockId;
					// The data identifier,
					int dataId = address.DataId;

					// Fetch the block container,
					BlockContainer container = GetBlock(touched, blockId);
					// Remove the data,
					container.Delete(dataId);
					// Schedule the block to be file synch'd 5 seconds after a write
					service.ScheduleBlockFlush(container, 5000);
				}
			}

			private long SendBlockTo(BlockId blockId, IServiceAddress destination,
									 long destServerGuid,
									 IServiceAddress[] managerAddress) {
				lock (service.processLock) {
					long processId = service.processId;
					service.processId = service.processId + 1;
					SendBlockInfo info = new SendBlockInfo(processId, blockId,
													  destination,
													  destServerGuid,
													  managerAddress);
					// Schedule the process to happen immediately (or as immediately as
					// possible).
					service.sendBlockTimer = new Timer(SendBlock, info, 0, Timeout.Infinite);

					return processId;
				}
			}

			private void SendBlock(object state) {
				SendBlockInfo info = (SendBlockInfo)state;

				BlockContainer container = service.FetchBlockContainer(info.blockId);
				if (!container.Exists)
					return;

				// Connect to the destination service address,
				IMessageProcessor p = service.connector.Connect(info.destination, ServiceType.Block);

				try {
					// If the block was written less than 6 minutes ago, we don't allow
					// the copy to happen,
					if (!service.IsKnownStaticBlock(container)) {
						service.Logger.Info("Can't copy last block_id ( " + info.blockId + " ) on server: not a known static block.");
						return;
					}

					Message response;
					Message request;

					// If the block does exist, push it over,
					byte[] buf = new byte[16384];
					int pos = 0;
					using (Stream input = container.OpenInputStream()) {
						int read;
						while ((read = input.Read(buf, 0, buf.Length)) != 0) {
							request = new RequestMessage("sendBlockPart");
							request.Arguments.Add(info.blockId);
							request.Arguments.Add(pos);
							request.Arguments.Add(container.Type);
							request.Arguments.Add(buf);
							request.Arguments.Add(read);
							// Process the message,
							response = p.Process(request);

							if (response.HasError) {
								service.Logger.Log(LogLevel.Error, "sendBlockPath Error: " + response.ErrorMessage);
								return;
							}

							pos += read;
						}
					}

					// Send the 'complete' command,
					request = new RequestMessage("sendBlockComplete");
					request.Arguments.Add(info.blockId);
					request.Arguments.Add(container.Type);

					// Process the message,
					response = p.Process(request);

					if (response.HasError) {
						service.Logger.Error("sendBlockComplete Error: " + response.ErrorMessage);
						return;
					}

					// Tell the manager service about this new block mapping,
					for (int i = 0; i < info.managerAddress.Length; i++) {
						IMessageProcessor mp = service.connector.Connect(info.managerAddress[i], ServiceType.Manager);
						request = new RequestMessage("internalAddBlockServerMapping");
						request.Arguments.Add(info.blockId);
						request.Arguments.Add(new long[] {info.destServerGuid});

						service.Logger.Info("Adding block_id->server mapping (" + info.blockId + " -> " + info.destServerGuid + ")");

						// Process the message,
						response = mp.Process(request);

						if (response.HasError) {
							service.Logger.Error("internalAddBlockServerMapping Error: " + response.ErrorMessage);
							return;
						}
					}
				} catch (IOException e) {
					service.Logger.Error("IO Error: " + e.Message);
				}
			}

			private long GetBlockChecksum(IDictionary<BlockId, BlockContainer> touched, BlockId blockId) {
				// Fetch the block container,
				BlockContainer container = GetBlock(touched, blockId);
				// Calculate the checksum value,
				return container.CreateChecksum();
			}

			// The nodes fetched in this message,
			private List<NodeId> readNodes;

			public Message Process(Message message) {
				Message response;
				readNodes = null;

				if (MessageStream.TryProcess(this, message, out response)) {
					readNodes = null;
					return response;
				}

				// The map of containers touched,
				Dictionary<BlockId, BlockContainer> containersTouched = new Dictionary<BlockId, BlockContainer>();

				RequestMessage request = (RequestMessage) message;
				response = request.CreateResponse();

				try {
					service.CheckErrorState();

					switch (request.Name) {
						case "bindWithManager":
						case "unbindWithManager":
							//TODO: is this needed for a block service?
							response.Arguments.Add(1L);
							break;
						case "serverGUID":
							response.Arguments.Add(service.serverGuid);
							break;
						case "writeToBlock": {
							DataAddress address = (DataAddress) request.Arguments[0].Value;
							byte[] buffer = (byte[]) request.Arguments[1].Value;
							int offset = request.Arguments[2].ToInt32();
							int length = request.Arguments[3].ToInt32();
							WriteToBlock(containersTouched, address, buffer, offset, length);
							response.Arguments.Add(1L);
							break;
						}
						case "readFromBlock": {
							if (readNodes == null)
								readNodes = new List<NodeId>();

							DataAddress address = (DataAddress) request.Arguments[0].Value;
							if (!readNodes.Contains(address.Value)) {
								NodeSet nodeSet = ReadFromBlock(containersTouched, address);
								response.Arguments.Add(nodeSet);
								foreach(NodeId node in nodeSet.NodeIds)
									readNodes.Add(node);
							}
							break;
						}
						case "rollbackNodes": {
							DataAddress[] addresses = (DataAddress[]) request.Arguments[0].Value;
							DeleteNodes(containersTouched, addresses);
							response.Arguments.Add(1L);
							break;
						}
						case "deleteBlock": {
							BlockId blockId = (BlockId) request.Arguments[0].Value;
							//TODO: Can we delete blocks? Only in extreme conditions ... but how?
							response.Arguments.Add(1L);
							break;
						}
						case "blockSetReport": {
							BlockId[] blockIds = service.ListBlocks();
							response.Arguments.Add(service.serverGuid);
							response.Arguments.Add(blockIds);
							break;
						}
						case "blockChecksum": {
								BlockId blockId = (BlockId) request.Arguments[0].Value;
								long checksum = GetBlockChecksum(containersTouched, blockId);
								response.Arguments.Add(checksum);
								break;
							}
						case "sendBlockTo": {
								// Returns immediately. There's currently no way to determine
								// when this process will happen or if it will happen.
								BlockId blockId = (BlockId) request.Arguments[0].Value;
								IServiceAddress destAddress = (IServiceAddress)request.Arguments[1].Value;
								long destServerGuid = request.Arguments[2].ToInt64();
								IServiceAddress[] managerAddress = (IServiceAddress[])request.Arguments[3].Value;
								long processId = SendBlockTo(blockId, destAddress, destServerGuid, managerAddress);
								response.Arguments.Add(processId);
								break;
							}
						case "sendBlockPart": {
								BlockId blockId = (BlockId) request.Arguments[0].Value;
								long pos = request.Arguments[1].ToInt64();
								int storeType = request.Arguments[2].ToInt32();
								byte[] buffer = (byte[])request.Arguments[3].Value;
								int length = request.Arguments[4].ToInt32();
								service.WriteBlockPart(blockId, pos, storeType, buffer, length);
								response.Arguments.Add(1L);
								break;
							}
						case "sendBlockComplete": {
								BlockId blockId = (BlockId) request.Arguments[0].Value;
								int storeType = request.Arguments[1].ToInt32();
								service.CompleteBlockWrite(blockId, storeType);
								response.Arguments.Add(1L);
								break;
							}
						case "poll": {
							response.Arguments.Add(1L);
							break;
						}
						case "notifyCurrentBlockId": {
							service.NotifyCurrentBlockId((BlockId) message.Arguments[0].Value);
							response.Arguments.Add(1L);
							break;
						}
						case "createAvailabilityMapForBlocks": {
							BlockId[] block_ids = (BlockId[]) message.Arguments[0].Value;
							byte[] map = service.CreateAvailabilityMap(block_ids);
							response.Arguments.Add(map);
							break;
						}
						default:
							throw new ApplicationException("Unknown message name: " + request.Name);
					}
				} catch (OutOfMemoryException e) {
					service.Logger.Error("Out of Memory");
					service.SetErrorState(e);
					throw;
				} catch (Exception e) {
					service.Logger.Error("Error while processing: " + e.Message, e);
					response.Arguments.Add(new MessageError(e));
				}

				// Release any containers touched,
				try {
					CloseContainers(containersTouched);
				} catch (Exception e) {
					service.Logger.Error("Error while closing containers: " + e.Message, e);
				}

				return response;
			}
		}
		
		#region SendBlockInfo
		
		private struct SendBlockInfo {
			public SendBlockInfo(long processId, BlockId blockId, IServiceAddress destination, 
			                     long destServerGuid, IServiceAddress[] managerAddress) {
				this.processId = processId;
				this.blockId = blockId;
				this.destination = destination;
				this.destServerGuid = destServerGuid;
				this.managerAddress = managerAddress;
			}

			public long processId;
			public readonly BlockId blockId;
			public readonly IServiceAddress destination;
			public readonly long destServerGuid;
			public readonly IServiceAddress[] managerAddress;
		}
		
		#endregion
		
		#endregion
		
		#region BlockContainer
		
		protected sealed class BlockContainer : IBlockStore, IComparable<BlockContainer> {
			public IBlockStore blockStore;
			public readonly BlockId blockId;
			private DateTime lastWrite = DateTime.MinValue;
			private int lockCount = 0;
			private int type;
			
			internal BlockContainer(BlockId blockId, IBlockStore blockStore) {
				this.blockId = blockId;
				this.blockStore = blockStore;
				type = blockStore.Type;
			}
			
			public DateTime LastWriteTime {
				get { return lastWrite; }
			}
			
			public IBlockStore BlockStore {
				get { return blockStore; }
			}
			
			public BlockId BlockId {
				get { return blockId; }
			}
			
			public bool Exists {
				get {
					lock(this) {
						return blockStore.Exists;
					}
				}
			}
			
			public int Type {
				get{ return type; }
			}
			
			public bool Open() {
				lock (this) {
					if (lockCount == 0) {
						++lockCount;
						return blockStore.Open();
					}
					++lockCount;
					return false;
				}
			}
			
			public void Close() {
				lock (this) {
					if (--lockCount == 0)
						blockStore.Close();
				}
			}
			
			public void ChangeStore(IBlockStore newStore) {
				lock (this) {
					if (lockCount > 0) {
						blockStore.Close();
						newStore.Open();
					}
					blockStore = newStore;
				}
			}

			public void TouchLastWrite() {
				lastWrite = DateTime.Now;
			}
			
			public void Write(int dataId, byte[] buffer, int offset, int length) {
				TouchLastWrite();
				lock (this) {
					blockStore.Write(dataId, buffer, offset, length);
				}
			}
			
			public int Read(int dataId, byte[] buffer, int offset, int length) {
				lock(this) {
					return blockStore.Read(dataId, buffer, offset, length);
				}
			}
			
			public Stream OpenInputStream() {
				lock(this) {
					return blockStore.OpenInputStream();
				}
			}
			
			public void Delete(int dataId) {
				TouchLastWrite();
				lock (this) {
					blockStore.Delete(dataId);
				}
			}
			
			public NodeSet GetNodeSet(int dataId) {
				lock (this) {
					return blockStore.GetNodeSet(dataId);
				}
			}
			
			public void Flush() {
				lock (this) {
					blockStore.Flush();
				}
			}
			
			public long CreateChecksum() {
				lock (this) {
					return blockStore.CreateChecksum();
				}
			}
			
			public int CompareTo(BlockContainer other) {
				return blockId.CompareTo(other.blockId);
			}
		}
		
		#endregion
	}
}