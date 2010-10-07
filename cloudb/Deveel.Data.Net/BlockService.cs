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
		private long lastBlockId = -1;
		private readonly IServiceConnector connector;
		
		private readonly Dictionary<long, BlockContainer> blockContainerCache;
		private readonly LinkedList<BlockContainer> blockContainerAccessList;
		private readonly LinkedList<BlockContainer> blocksPendingFlush;
		private Timer blockFlushTimer;
		private readonly object pathLock = new object();
		
		private readonly object processLock = new object();
		private readonly object blockUploadLock = new object();
		private int processId;
		private Timer sendBlockTimer;

		private Logger log;
		
		protected BlockService(IServiceConnector connector) {
			this.connector = connector;
			
			blockContainerCache = new Dictionary<long, BlockContainer>(5279);
			blockContainerAccessList = new LinkedList<BlockContainer>();
			blocksPendingFlush = new LinkedList<BlockContainer>();
			
			log = LogManager.NetworkLogger;
		}
		
		public override ServiceType ServiceType {
			get { return ServiceType.Block; }
		}

		protected long LastBlockId {
			get { return lastBlockId; }
		}
		
		public int BlockCount {
			get { return blockCount; }
		}

		protected object BlockUploadSyncRoot {
			get { return blockUploadLock; }
		}
				
		private BlockContainer FetchBlockContainer(long block_id) {
			// Check for stop state,
			CheckErrorState();

			BlockContainer container;

			lock (pathLock) {
				// Look up the block container in the map,
				// If the container not in the map, create it and put it in there,
				if (!blockContainerCache.TryGetValue(block_id, out container)) {
					container = LoadBlock(block_id);
					// Put it in the map,
					blockContainerCache[block_id] = container;
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
					log.Warning("Error while flushing block content: " + e.Message);
				}
			}
		}
		
		private void WriteBlockPart(long blockId, long pos, int storeType, byte[] buffer, int length) {
			// Make sure this process is exclusive
			lock (blockUploadLock) {
				try {
					OnWriteBlockPart(blockId, pos, storeType, buffer, length);
				} catch (IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}

		}
		
		private void CompleteBlockWrite(long blockId, int storeType) {
			// Make sure this process is exclusive
			lock (blockUploadLock) {
				OnCompleteBlockWrite(blockId, storeType);
			}

			// Update internal state as appropriate,
			lock (pathLock) {
				BlockContainer container = LoadBlock(blockId);
				blockContainerCache[blockId] = container;
				if (blockId > lastBlockId) {
					lastBlockId = blockId;
				}
				++blockCount;
			}

		}
		
		private void CloseContainers() {
			lock(pathLock) {
				foreach(KeyValuePair<long, BlockContainer> pair in blockContainerCache) {
					pair.Value.BlockStore.Close();
				}
			}
		}

		protected override void OnDispose(bool disposing) {
			if (disposing) {
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
				lastBlockId = -1;
			}
		}
		
		protected void SetGuid(long value) {
			serverGuid = value;
		}
						
		protected virtual void OnCompleteBlockWrite(long blockId, int storeType) {
		}
		
		protected virtual void OnWriteBlockPart(long blockId, long pos, int storeType, byte[] buffer, int length) {
			
		}
		
		protected abstract BlockContainer LoadBlock(long blockId);
		
		protected BlockContainer GetBlock(long blockId) {
			CheckErrorState();
			
			BlockContainer container;
			lock(pathLock) {
				if (!blockContainerCache.TryGetValue(blockId, out container))
					return null;
			}
			
			return container;
		}
		
		protected abstract long[] ListBlocks();

		protected override IMessageProcessor CreateProcessor() {
			return new BlockServiceMessageProcessor(this);
		}

		protected override void OnInit() {
			// Read in all the blocks in and populate the map,
			long[] blocks = ListBlocks();
			long inLastBlockId = -1;

			lock (pathLock) {
				foreach (long blockId in blocks) {
					BlockContainer container = LoadBlock(blockId);
					blockContainerCache[blockId] = container;
					if (blockId > inLastBlockId) {
						inLastBlockId = blockId;
					}
				}
			}

			// Discover the block count,
			blockCount = blocks.Length;
			// The latest block on this service,
			this.lastBlockId = inLastBlockId;
		}
		
		#region BlockServerMessageProcessor

		private sealed class BlockServiceMessageProcessor : IMessageProcessor {
			private readonly BlockService service;

			public BlockServiceMessageProcessor(BlockService service) {
				this.service = service;
			}

			private void CloseContainers(Dictionary<long, BlockContainer> touched) {
				foreach (KeyValuePair<long, BlockContainer> c in touched) {
					service.log.Log(LogLevel.Information, service, "Closing block (ID: " + c.Key + ")");
					c.Value.Close();
				}
			}

			private BlockContainer GetBlock(IDictionary<long, BlockContainer> touched, long blockId) {
				BlockContainer b;
				if (!touched.TryGetValue(blockId, out b)) {
					b = service.FetchBlockContainer(blockId);
					bool created = b.Open();
					if (created) {
						++service.blockCount;
						service.lastBlockId = blockId;
					}
					touched[blockId] = b;
					service.log.Log(LogLevel.Information, service, "Opening block (ID: " + blockId+")");
				}
				return b;
			}

			private void WriteToBlock(Dictionary<long, BlockContainer> touched, DataAddress address, byte[] buf, int off, int len) {
				// The block being written to,
				long blockId = address.BlockId;
				// The data identifier,
				int dataId = address.DataId;

				// Fetch the block container,
				BlockContainer container = GetBlock(touched, blockId);
				// Write the data,
				container.Write(dataId, buf, off, len);

				// Schedule the block to flushed 5 seconds after a write
				service.ScheduleBlockFlush(container, 5000);
			}

			private NodeSet ReadFromBlock(IDictionary<long, BlockContainer> touched, DataAddress address) {
				// The block being written to,
				long blockId = address.BlockId;
				// The data identifier,
				int dataId = address.DataId;

				// Fetch the block container,
				BlockContainer container = GetBlock(touched, blockId);
				// Read the data,
				return container.GetNodeSet(dataId);
			}

			private void DeleteNodes(IDictionary<long, BlockContainer> touched, DataAddress[] addresses) {
				foreach (DataAddress address in addresses) {
					// The block being removed from,
					long blockId = address.BlockId;
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

			private long SendBlockTo(long blockId, IServiceAddress destination,
									 long destServerGuid,
									 IServiceAddress managerAddress) {
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
					// If the block was accessed less than 5 minutes ago, we don't allow
					// the copy to happen,
					if (info.blockId == service.lastBlockId) {
						service.log.Info(String.Format("Can't copy last block_id ( {0} ) on server.", info.blockId));
						return;
					} else if (container.LastWriteTime > 
					           DateTime.Now.AddMilliseconds(-(6 * 60 * 1000))) {
						// Won't copy a block that was written to within the last 6 minutes,
						service.log.Info(String.Format("Can't copy block ( {0} ) written to within the last 6 minutes.", info.blockId));
						return;
					}

					MessageRequest request;
					Message response;

					// If the block does exist, push it over,
					byte[] buf = new byte[16384];
					int pos = 0;
					using(Stream input = container.OpenInputStream()) {
						int read;
						while ((read = input.Read(buf, 0, buf.Length)) != 0) {
							request = new MessageRequest("sendBlockPart");
							request.Arguments.Add(info.blockId);
							request.Arguments.Add(pos);
							request.Arguments.Add(container.Type);
							request.Arguments.Add(buf);
							request.Arguments.Add(read);
							
							// Process the request,
							response = p.ProcessMessage(request);

								if (response.HasError) {
									service.log.Log(LogLevel.Error, "sendBlockPath Error: " + response.ErrorMessage);
									return;
								}
	
							pos += read;
						}
					}
					
					// Send the 'complete' command,
					request = new MessageRequest("sendBlockComplete");
					request.Arguments.Add(info.blockId);
					request.Arguments.Add(container.Type);

					// Process the response,
					response = p.ProcessMessage(request);

					if (response.HasError) {
						service.log.Error("sendBlockComplete Error: " + response.ErrorMessage);
						return;
					}

					// Tell the manager service about this new block mapping,
					IMessageProcessor mp = service.connector.Connect(info.managerAddress, ServiceType.Manager);
					request = new MessageRequest("addBlockServerMapping");
					request.Arguments.Add(info.blockId);
					request.Arguments.Add(info.destServerGuid);
					
					service.log.Info(String.Format("Adding block_id->server mapping ({0} -> {1})", info.blockId, info.destServerGuid));

					// Process the message,
					response = mp.ProcessMessage(request);

					if (response.HasError) {
						service.log.Error("addBlockServerMapping Error: " + response.ErrorMessage);
						return;
					}

				} catch (IOException e) {
					service.log.Error("IO Error: " + e.Message);
				}
			}

			private long GetBlockChecksum(IDictionary<long, BlockContainer> touched, long blockId) {
				// Fetch the block container,
				BlockContainer container = GetBlock(touched, blockId);
				// Calculate the checksum value,
				return container.CreateChecksum();
			}

			public Message ProcessMessage(Message message) {
				if (message is MessageStream) {
					MessageStream stream = (MessageStream) message;
					MessageStream responseStream = new MessageStream(MessageType.Response);
					foreach(Message msg in stream) {
						responseStream.AddMessage(ProcessMessage(msg));
					}

					return responseStream;
				}

				// The map of containers touched,
				Dictionary<long, BlockContainer> containersTouched = new Dictionary<long, BlockContainer>();
				// The nodes fetched in this message,
				List<long> readNodes = null;

				MessageRequest request = (MessageRequest) message;
				MessageResponse response = null;

				try {
					service.CheckErrorState();

					switch (request.Name) {
						case "bindWithManager":
						case "unbindWithManager":
							//TODO: is this needed for a block service?
							response = request.CreateResponse("R");
							response.Arguments.Add(1L);
							break;
						case "serverGUID":
							response = request.CreateResponse("R");
							response.Arguments.Add(service.serverGuid);
							break;
						case "writeToBlock": {
								DataAddress address = (DataAddress)request.Arguments[0].Value;
								byte[] buffer = (byte[])request.Arguments[1].Value;
								int offset = request.Arguments[2].ToInt32();
								int length = request.Arguments[3].ToInt32();
								WriteToBlock(containersTouched, address, buffer, offset, length);
							response = request.CreateResponse("R");
								response.Arguments.Add(1L);
								break;
							}
						case "readFromBlock": {
								if (readNodes == null)
									readNodes = new List<long>();

								DataAddress address = (DataAddress)request.Arguments[0].Value;
								if (!readNodes.Contains(address.Value)) {
									NodeSet nodeSet = ReadFromBlock(containersTouched, address);
									response = request.CreateResponse("R");
									response.Arguments.Add(nodeSet);
									foreach (long node in nodeSet.NodeIds)
										readNodes.Add(node);
								}
								break;
							}
						case "rollbackNodes": {
								DataAddress[] addresses = (DataAddress[])request.Arguments[0].Value;
								DeleteNodes(containersTouched, addresses);
								response = request.CreateResponse("R");
								response.Arguments.Add(1L);
								break;
							}
						case "deleteBlock": {
								long blockId = request.Arguments[0].ToInt64();
								//TODO: ...
								response = request.CreateResponse("R");
								response.Arguments.Add(1L);
								break;
							}
						case "blockSetReport": {
							long[] blockIds = service.ListBlocks();
							response = request.CreateResponse("R");
							response.Arguments.Add(service.serverGuid);
							response.Arguments.Add(blockIds);
							break;
						}
						case "blockChecksum": {
								long blockId = request.Arguments[0].ToInt64();
								long checksum = GetBlockChecksum(containersTouched, blockId);
							response = request.CreateResponse("R");
								response.Arguments.Add(checksum);
								break;
							}
						case "sendBlockTo": {
								// Returns immediately. There's currently no way to determine
								// when this process will happen or if it will happen.
								long blockId = request.Arguments[0].ToInt64();
								IServiceAddress destAddress = (IServiceAddress)request.Arguments[1].Value;
								long destServerGuid = request.Arguments[2].ToInt64();
								IServiceAddress managerAddress = (IServiceAddress)request.Arguments[3].Value;
								long processId = SendBlockTo(blockId, destAddress, destServerGuid, managerAddress);
							response = request.CreateResponse("R");
								response.Arguments.Add(processId);
								break;
							}
						case "sendBlockPart": {
								long blockId = request.Arguments[0].ToInt64();
								long pos = request.Arguments[1].ToInt64();
								int storeType = request.Arguments[2].ToInt32();
								byte[] buffer = (byte[])request.Arguments[3].Value;
								int length = request.Arguments[4].ToInt32();
								service.WriteBlockPart(blockId, pos, storeType, buffer, length);
							response = request.CreateResponse("R");
								response.Arguments.Add(1L);
								break;
							}
						case "sendBlockComplete": {
								long blockId = request.Arguments[0].ToInt64();
								int storeType = request.Arguments[1].ToInt32();
								service.CompleteBlockWrite(blockId, storeType);
							response = request.CreateResponse("R");
								response.Arguments.Add(1L);
								break;
							}
							case "poll": {
								response = request.CreateResponse("R");
								response.Arguments.Add(1L);
								break;
							}
						default:
							throw new ApplicationException("Unknown message name: " + request.Name);
					}
				} catch (OutOfMemoryException e) {
					service.log.Error("Out of Memory");
					service.SetErrorState(e);
					throw;
				} catch (Exception e) {
					service.log.Error("Error while processing: " + e.Message, e);
					response = request.CreateResponse("E");
					response.Code = MessageResponseCode.Error;
					response.Arguments.Add(new MessageError(e));
				}

				// Release any containers touched,
				try {
					CloseContainers(containersTouched);
				} catch (Exception e) {
					service.log.Error("Error while closing containers: " + e.Message, e);
				}

				return response;
			}
		}
		
		#region SendBlockInfo
		
		private struct SendBlockInfo {
			public SendBlockInfo(long processId, long blockId, IServiceAddress destination, 
			                     long destServerGuid, IServiceAddress managerAddress) {
				this.processId = processId;
				this.blockId = blockId;
				this.destination = destination;
				this.destServerGuid = destServerGuid;
				this.managerAddress = managerAddress;
			}

			public long processId;
			public long blockId;
			public IServiceAddress destination;
			public long destServerGuid;
			public IServiceAddress managerAddress;
		}
		
		#endregion
		
		#endregion
		
		#region BlockContainer
		
		protected sealed class BlockContainer : IBlockStore, IComparable<BlockContainer> {
			public IBlockStore blockStore;
			public readonly long blockId;
			private DateTime lastWrite = DateTime.MinValue;
			private int lockCount = 0;
			private int type;
			
			internal BlockContainer(long blockId, IBlockStore blockStore) {
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
			
			public long BlockId {
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
			
			public void Write(int dataId, byte[] buffer, int offset, int length) {
				lock (this) {
					blockStore.Write(dataId, buffer, offset, length);
					lastWrite = DateTime.Now;
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
				lock (this) {
					blockStore.Delete(dataId);
					lastWrite = DateTime.Now;
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