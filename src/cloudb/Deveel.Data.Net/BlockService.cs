using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public abstract class BlockService : Service {
		private readonly IServiceConnector connector;

		private readonly Dictionary<BlockId, BlockContainer> blockContainerMap;
		private readonly LinkedList<BlockContainer> blockContainerAccessList; 

		private readonly object processLock = new object();
		private readonly object blockUploadLock = new object();
		private readonly object pathLock = new object();

		private long processIdSeq = 10;

		private long serverGuid;

		private long blockCount = 0;

		private readonly LinkedList<BlockContainer> blocksPendingSync;

		private readonly Dictionary<int, BlockId> maxKnownBlockId;

		protected BlockService(IServiceConnector connector) {
			this.connector = connector;

			blockContainerMap = new Dictionary<BlockId, BlockContainer>();
			blockContainerAccessList = new LinkedList<BlockContainer>();

			maxKnownBlockId = new Dictionary<int, BlockId>();

			blocksPendingSync = new LinkedList<BlockContainer>();
		}


		public override ServiceType ServiceType {
			get { return ServiceType.Block; }
		}

		protected override IMessageProcessor CreateProcessor() {
			CheckErrorState();
			return new MessageProcessor(this);
		}

		protected IServiceConnector Connector {
			get { return connector; }
		}

		public long Id {
			get { return serverGuid; }
			protected set { serverGuid = value; }
		}

		public long BlockCount {
			get { return blockCount; }
		}

		protected override void OnStop() {
			lock(pathLock)
			{
				blockContainerMap.Clear();
				blocksPendingSync.Clear();
				blockContainerAccessList.Clear();
			}

			blockCount = 0;
		}

		protected override void OnStart() {
			// Read in all the blocks in and populate the map,
			BlockId[] blocks = FetchBlockList();

			lock (pathLock) {
				foreach (BlockId blockId in blocks) {
					BlockContainer container = LoadBlock(blockId);
					blockContainerMap[blockId] = container;
				}
			}


			// Discover the block count,
			blockCount = blocks.Length;

			base.OnStart();
		}

		protected virtual BlockId[] FetchBlockList() {
			return new BlockId[0];
		}

		private void NotifyCurrentBlockId(BlockId blockId) {
			// Extract the low byte out of the block id, which is a key for the manager
			// server that generated this block chain.
			int managerKey = ((int) blockId.Low & 0x0FF);

			// Update the map for this key,
			lock (maxKnownBlockId) {
				maxKnownBlockId[managerKey] = blockId;
			}
		}

		protected bool IsKnownStaticBlock(BlockContainer block) {

			// If the block was written to less than 3 minutes ago, return false
			if (block.LastWrite > DateTime.Now.AddMinutes(-(3*60*1000))) {
				return false;
			}
			// Extract the server key from the block id
			BlockId blockId = block.Id;
			int serverId = ((int) blockId.Low & 0x0FF);

			// Look up the max known block id for the manager server,
			BlockId maxBlockId;
			lock (maxKnownBlockId) {
				maxKnownBlockId.TryGetValue(serverId, out maxBlockId);
			}

			// If the block is less than the max, the block can be compressed!
			if (maxBlockId != null && blockId.CompareTo(maxBlockId) < 0) {
				return true;
			}
			// Otherwise update the last write flag (so we only check the max block id
			// every 3 mins).
			block.TouchLastWrite();
			return false;
		}

		private void WriteBlockPart(BlockId blockId, long pos, int fileType, byte[] buf, int bufSize) {
			// Make sure this process is exclusive
			lock (blockUploadLock) {
				try {
					BlockData data = GetBlockData(blockId, fileType);
					if (pos == 0) {
						if (data.Exists)
							throw new ApplicationException("Block data exists.");

						data.Create();
					}

					if (data.Length != pos)
						throw new ApplicationException("Block sync issue on block file.");

					// Everything ok, we can write the file,
					Stream fout = data.OpenWrite();
					fout.Write(buf, 0, bufSize);
					fout.Close();

				} catch (IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
		}

		private void WriteBlockComplete(BlockId blockId, int fileType) {
			BlockData data = GetBlockData(blockId, fileType);

			// Make sure this process is exclusive
			lock (blockUploadLock) {
				if (!data.Exists)
					throw new ApplicationException("Block data not found");

				data.Complete();
			}

			UpdateBlockState(blockId);
		}

		private void UpdateBlockState(BlockId blockId) {
			// Update internal state as appropriate,
			lock (pathLock) {
				BlockContainer container = LoadBlock(blockId);
				blockContainerMap[blockId] = container;
				++blockCount;
			}
		}

		protected abstract BlockData GetBlockData(BlockId blockId, int blockType);

		protected abstract IBlockStore GetBlockStore(BlockId blockId);

		protected virtual void OnBlockLoaded(BlockContainer container) {
		}

		private BlockContainer LoadBlock(BlockId blockId) {
			IBlockStore blockStore = GetBlockStore(blockId);

			// Make the block container object,
			BlockContainer container = new BlockContainer(blockId, blockStore);

			OnBlockLoaded(container);

			return container;
		}

		private BlockContainer FetchBlockContainer(BlockId blockId) {
			// Check for error state,
			CheckErrorState();

			lock (pathLock) {
				// Look up the block container in the map,
				BlockContainer container;
				// If the container not in the map, create it and put it in there,
				if (!blockContainerMap.TryGetValue(blockId, out container)) {

					container = LoadBlock(blockId);
					// Put it in the map,
					blockContainerMap[blockId] = container;
				}

				// We manage a small list of containers that have been accessed ordered
				// by last access time.

				// Iterate through the list. If we discover the BlockContainer recently
				// accessed we move it to the front.
				LinkedListNode<BlockContainer> node = blockContainerAccessList.First;

				while (node != null) {
					var next = node.Next;
					if (node.Value == container) {
						// Found, so move it to the front,
						blockContainerAccessList.Remove(node);
						blockContainerAccessList.AddFirst(container);
						return container;
					}
					node = next;
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

		private byte[] CreateAvailabilityMapForBlocks(BlockId[] blocks) {
			byte[] result = new byte[blocks.Length];

			// Use the OS filesystem file name lookup to determine if the block is
			// stored here or not.

			for (int i = 0; i < blocks.Length; ++i) {
				bool found = true;
				/*
				OLD:
				// Turn the block id into a filename,
				string blockFname = FormatFileName(blocks[i]);
				// Check for the compressed filename,
				string blockFileName = Path.Combine(path, blockFname + ".mcd");
				if (!File.Exists(blockFileName)) {
					// Check for the none-compressed filename
					blockFileName = Path.Combine(path, blockFname);
					// If this file doesn't exist,
					if (!File.Exists(blockFileName))
						found = false;
				}
				*/

				found = DiscoverBlockType(blocks[i]) != -1;
				// Set the value in the map
				result[i] = found ? (byte)1 : (byte)0;
			}

			return result;
		}

		private void ScheduleFileFlush(BlockContainer container, int delay) {
			lock (pathLock) {
				if (!blocksPendingSync.Contains(container)) {
					blocksPendingSync.AddFirst(container);

					new Timer(FileFlushTask, container, delay, Timeout.Infinite);
				}
			}
		}

		private void FileFlushTask(object state) {
			BlockContainer container = (BlockContainer) state;
			lock (pathLock) {
				blocksPendingSync.Remove(container);
				try {
					container.Flush();
				} catch (IOException e) {
					Logger.Warning(String.Format("Sync error: {0}", e.Message));
					// We log the warning, but otherwise ignore any IO Error on a
					// file synchronize.
				}
			}
		}

		protected virtual int DiscoverBlockType(BlockId blockId) {
			return 1;
		}

		#region BlockData

		protected abstract class BlockData {
			private readonly BlockId blockId;
			private readonly int blockType;

			protected BlockData(BlockId blockId, int blockType) {
				this.blockId = blockId;
				this.blockType = blockType;
			}

			public int BlockType {
				get { return blockType; }
			}

			public BlockId BlockId {
				get { return blockId; }
			}

			public abstract bool Exists { get; }

			public virtual long Length {
				get { return 0; }
			}

			public abstract Stream OpenRead();

			public abstract Stream OpenWrite();

			public virtual void Complete() {
			}

			public virtual void Create() {
			}
		}

		#endregion

		#region BlockContainer

		protected class BlockContainer : IComparable<BlockContainer> {
			private IBlockStore blockStore;
			private bool isCompressed;
			private readonly BlockId blockId;

			private DateTime? lastWrite;

			private int lockCount;

			internal BlockContainer(BlockId blockId, IBlockStore blockStore) {
				this.blockId = blockId;
				if (blockStore is FileBlockStore) {
					isCompressed = false;
				} else if (blockStore is CompressedBlockStore) {
					isCompressed = true;
				} else {
					throw new ApplicationException("Unknown block_store type");
				}
				this.blockStore = blockStore;
			}


			public DateTime? LastWrite {
				get { return lastWrite; }
			}

			public bool IsCompressed {
				get { return isCompressed; }
			}

			public BlockId Id {
				get { return blockId; }
			}

			public IBlockStore Store {
				get { return blockStore; }
			}

			public void TouchLastWrite() {
				lastWrite = DateTime.Now;
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
					--lockCount;
					if (lockCount == 0) {
						blockStore.Close();
					}
				}
			}

			public void ChangeStore(IBlockStore newStore) {
				lock (this) {
					if (lockCount > 0) {
						blockStore.Close();
						newStore.Open();
					}
					if (newStore is FileBlockStore) {
						isCompressed = false;
					} else if (newStore is CompressedBlockStore) {
						isCompressed = true;
					} else {
						throw new ApplicationException("Unknown block_store type");
					}
					blockStore = newStore;
				}
			}

			public void Write(int dataId, byte[] buf, int off, int len) {
				TouchLastWrite();
				lock (this) {
					blockStore.Write(dataId, buf, off, len);
				}
			}

			public NodeSet Read(int dataId) {
				lock (this) {
					return blockStore.GetNodeSet(dataId);
				}
			}

			public void Remove(int dataId) {
				TouchLastWrite();
				lock (this) {
					blockStore.Delete(dataId);
				}
			}

			public long CreateChecksum() {
				lock (this) {
					return blockStore.CreateChecksum();
				}
			}

			public void Flush() {
				lock (this) {
					blockStore.Flush();
				}
			}

			public int CompareTo(BlockContainer other) {
				return blockId.CompareTo(other.blockId);
			}
		}

		#endregion

		#region MessageProcessor

		private class MessageProcessor : IMessageProcessor {
			private readonly BlockService service;

			public MessageProcessor(BlockService service) {
				this.service = service;
			}

			private BlockContainer GetBlock(Dictionary<BlockId, BlockContainer> touched, BlockId blockId) {
				BlockContainer b;
				if (!touched.TryGetValue(blockId, out b)) {
					b = service.FetchBlockContainer(blockId);
					bool created = b.Open();
					if (created) {
						++service.blockCount;
					}
					touched[blockId] = b;
				}
				return b;
			}

			private void CloseContainers(Dictionary<BlockId, BlockContainer> touched) {
				foreach (BlockContainer c in touched.Values) {
					c.Close();
				}
			}


			public IEnumerable<Message> Process(IEnumerable<Message> stream) {
				// The map of containers touched,
				Dictionary<BlockId, BlockContainer> containersTouched = new Dictionary<BlockId, BlockContainer>();
				// The reply message,
				MessageStream replyMessage = new MessageStream();
				// The nodes fetched in this message,
				List<NodeId> readNodes = null;

				foreach (Message m in stream) {
					try {
						// Check for stop state,
						service.CheckErrorState();

						// writeToBlock(DataAddress, byte[], int, int)
						if (m.Name.Equals("writeToBlock")) {
							WriteToBlock(containersTouched,
							             (DataAddress) m.Arguments[0].Value,
							             (byte[]) m.Arguments[1].Value, (int) m.Arguments[2].Value,
							             (int) m.Arguments[3].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// readFromBlock(DataAddress)
						else if (m.Name.Equals("readFromBlock")) {
							if (readNodes == null) {
								readNodes = new List<NodeId>();
							}
							DataAddress addr = (DataAddress) m.Arguments[0].Value;
							if (!readNodes.Contains(addr.Value)) {
								NodeSet nodeSet = ReadFromBlock(containersTouched, addr);
								replyMessage.AddMessage(new Message(nodeSet));
								readNodes.AddRange(nodeSet.NodeIds);
							}
						}
							// rollbackNodes(DataAddress[] )
						else if (m.Name.Equals("rollbackNodes")) {
							RemoveNodes(containersTouched, (DataAddress[]) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// deleteBlock(BlockId)
						else if (m.Name.Equals("deleteBlock")) {
							DeleteBlock((BlockId) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// serverGUID()
						else if (m.Name.Equals("serverGUID")) {
							replyMessage.AddMessage(new Message(service.Id));
						}
							// blockSetReport()
						else if (m.Name.Equals("blockSetReport")) {
							BlockId[] arr = BlockSetReport();
							replyMessage.AddMessage(new Message(service.Id, arr));
						}
							// poll(String)
						else if (m.Name.Equals("poll")) {
							replyMessage.AddMessage(new Message(1L));
						}

							// notifyCurrentBlockId(BlockId)
						else if (m.Name.Equals("notifyCurrentBlockId")) {
							service.NotifyCurrentBlockId((BlockId) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1L));
						}

							// blockChecksum(BlockId)
						else if (m.Name.Equals("blockChecksum")) {
							long checksum = BlockChecksum(containersTouched, (BlockId) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(checksum));
						}
							// sendBlockTo(BlockId, IServiceAddress, long, IServerAddress[])
						else if (m.Name.Equals("sendBlockTo")) {
							// Returns immediately. There's currently no way to determine
							// when this process will happen or if it will happen.
							BlockId blockId = (BlockId) m.Arguments[0].Value;
							IServiceAddress destAddress = (IServiceAddress) m.Arguments[1].Value;
							long destServerId = (long) m.Arguments[2].Value;
							IServiceAddress[] managerServers = (IServiceAddress[]) m.Arguments[3].Value;
							long processId = SendBlockTo(blockId, destAddress, destServerId, managerServers);
							replyMessage.AddMessage(new Message(processId));
						}
							// sendBlockPart(BlockId, long, int, byte[], int)
						else if (m.Name.Equals("sendBlockPart")) {
							BlockId blockId = (BlockId) m.Arguments[0].Value;
							long pos = (long) m.Arguments[1].Value;
							int fileType = (int) m.Arguments[2].Value;
							byte[] buf = (byte[]) m.Arguments[3].Value;
							int bufSize = (int) m.Arguments[4].Value;
							service.WriteBlockPart(blockId, pos, fileType, buf, bufSize);
							replyMessage.AddMessage(new Message(1L));
						}
							// sendBlockComplete(BlockId, int)
						else if (m.Name.Equals("sendBlockComplete")) {
							BlockId blockId = (BlockId) m.Arguments[0].Value;
							int fileType = (int) m.Arguments[1].Value;
							service.WriteBlockComplete(blockId, fileType);
							replyMessage.AddMessage(new Message(1L));
						}

							// createAvailabilityMapForBlocks(BlockId[])
						else if (m.Name.Equals("createAvailabilityMapForBlocks")) {
							BlockId[] blockIds = (BlockId[]) m.Arguments[0].Value;
							byte[] map = service.CreateAvailabilityMapForBlocks(blockIds);
							replyMessage.AddMessage(new Message(map));
						}

							// bindWithManager()
						else if (m.Name.Equals("bindWithManager")) {
							BindWithManager();
							replyMessage.AddMessage(new Message(1L));
						}
							// unbindWithManager()
						else if (m.Name.Equals("unbindWithManager")) {
							UnbindWithManager();
							replyMessage.AddMessage(new Message(1L));
						} else {
							throw new ApplicationException("Unknown command: " + m.Name);
						}

					} catch (OutOfMemoryException e) {
						service.Logger.Error("Memory Error", e);
						service.SetErrorState(e);
						throw;
					} catch (Exception e) {
						service.Logger.Error("Exception during process", e);
						replyMessage.AddMessage(new Message(new MessageError(e)));
					}
				}

				// Release any containers touched,
				try {
					CloseContainers(containersTouched);
				} catch (IOException e) {
					service.Logger.Error("IOError when closing containers", e);
				}

				return replyMessage;

			}

			private BlockId[] BlockSetReport() {
				return service.FetchBlockList();
			}

			private void DeleteBlock(BlockId blockId) {
			}

			private void RemoveNodes(Dictionary<BlockId, BlockContainer> containersTouched, IEnumerable<DataAddress> addresses) {
				foreach (DataAddress address in addresses) {
					// The block being removed from,
					BlockId blockId = address.BlockId;
					// The data identifier,
					int dataId = address.DataId;

					// Fetch the block container,
					BlockContainer container = GetBlock(containersTouched, blockId);
					// Remove the data,
					container.Remove(dataId);
					// Schedule the block to be file synch'd 5 seconds after a write
					service.ScheduleFileFlush(container, 5000);
				}
			}

			private long SendBlockTo(BlockId blockId, IServiceAddress destAddress, long destServerId,
			                         IServiceAddress[] managerServers) {
				lock (service.processLock) {
					long processId = service.processIdSeq++;
					SendBlockInfo info = new SendBlockInfo(processId, blockId, destAddress, destServerId, managerServers);
					// Schedule the process to happen immediately (or as immediately as
					// possible).
					new Timer(SendBlockToTask, info, 0, Timeout.Infinite);

					return processId;
				}
			}

			#region SendBlockInfo

			private class SendBlockInfo {
				private readonly long processId;
				private readonly BlockId blockId;
				private readonly IServiceAddress destAddress;
				private readonly long destServerId;
				private readonly IServiceAddress[] managerServers;

				public SendBlockInfo(long processId, BlockId blockId, IServiceAddress destAddress, long destServerId,
				                     IServiceAddress[] managerServers) {
					this.processId = processId;
					this.blockId = blockId;
					this.destAddress = destAddress;
					this.destServerId = destServerId;
					this.managerServers = managerServers;
				}

				public IServiceAddress[] ManagerServers {
					get { return managerServers; }
				}

				public long DestinationServerId {
					get { return destServerId; }
				}

				public IServiceAddress Destination {
					get { return destAddress; }
				}

				public BlockId BlockId {
					get { return blockId; }
				}

				public long ProcessId {
					get { return processId; }
				}
			}

			#endregion

			private void SendBlockToTask(object state) {
				SendBlockInfo info = (SendBlockInfo) state;

				// Connect to the destination service address,
				IMessageProcessor p = service.Connector.Connect(info.Destination, ServiceType.Block);

				int blockType = service.DiscoverBlockType(info.BlockId);
				if (blockType == -1)
					return;

				BlockData data = service.GetBlockData(info.BlockId, blockType);

				// If the data was not found, exit,
				if (!data.Exists)
					return;

				try {

					BlockContainer blockContainer = service.FetchBlockContainer(info.BlockId);
					// If the block was written to less than 6 minutes ago, we don't allow
					// the copy to happen,
					if (!service.IsKnownStaticBlock(blockContainer)) {
						// This will happen if this block server has not be notified
						// recently by the managers the maximum block id they are managing.
						service.Logger.Info(String.Format("Can't copy last block_id ( {0} ) on server, it's not a known static block.",
						                                  info.BlockId));
						return;
					}


					IEnumerable<Message> inputStream;
					Message ouputMessage;

					// If the file does exist, push it over,
					byte[] buf = new byte[16384];
					int pos = 0;
					Stream fin = data.OpenRead();

					while (true) {
						int read = fin.Read(buf, 0, buf.Length);
						// Exit if we reached the end of the file,
						if (read == 0)
							break;

						ouputMessage = new Message("sendBlockPart", info.BlockId, pos, blockType, buf, read);
						// Process the message,
						inputStream = p.Process(ouputMessage.AsStream());
						// Get the input iterator,
						foreach (Message m in inputStream) {
							if (m.HasError) {
								service.Logger.Info(String.Format("'sendBlockPart' command error: {0}", m.ErrorMessage));
								return;
							}
						}

						pos += read;
					}

					// Close,
					fin.Close();

					// Send the 'complete' command,
					ouputMessage = new Message("sendBlockComplete", info.BlockId, blockType);
					// Process the message,
					inputStream = p.Process(ouputMessage.AsStream());
					// Get the input iterator,
					foreach (Message m in inputStream) {
						if (m.HasError) {
							service.Logger.Info(String.Format("'sendBlockCommand' command error: {0}", m.ErrorMessage));
							return;
						}
					}

					// Tell the manager server about this new block mapping,
					ouputMessage = new Message("internalAddBlockServerMapping", info.BlockId, new long[] {info.DestinationServerId});
					service.Logger.Info(String.Format("Adding block_id->server mapping ({0} -> {1})", info.BlockId,
					                                  info.DestinationServerId));

					for (int n = 0; n < info.ManagerServers.Length; ++n) {
						// Process the message,
						IMessageProcessor mp = service.Connector.Connect(info.ManagerServers[n], ServiceType.Manager);
						inputStream = mp.Process(ouputMessage.AsStream());
						// Get the input iterator,
						foreach (Message m in inputStream) {
							if (m.HasError) {
								service.Logger.Info(String.Format("'internalAddBlockServerMapping' command error: @ {0} - {1}",
								                                  info.ManagerServers[n], m.ErrorMessage));
								break;
							}
						}
					}
				} catch (IOException e) {
					service.Logger.Info(e);
				}
			}

			private NodeSet ReadFromBlock(Dictionary<BlockId, BlockContainer> containersTouched, DataAddress address) {
				// The block being written to,
				BlockId blockId = address.BlockId;
				// The data identifier,
				int dataId = address.DataId;

				// Fetch the block container,
				BlockContainer container = GetBlock(containersTouched, blockId);
				// Read the data,
				return container.Read(dataId);
			}

			private void WriteToBlock(Dictionary<BlockId, BlockContainer> containersTouched, DataAddress address, byte[] buffer,
			                          int offset, int length) {
				// The block being written to,
				BlockId blockId = address.BlockId;
				// The data identifier,
				int dataId = address.DataId;

				// Fetch the block container,
				BlockContainer container = GetBlock(containersTouched, blockId);
				// Write the data,
				container.Write(dataId, buffer, offset, length);

				// Schedule the block to be file synch'd 5 seconds after a write
				service.ScheduleFileFlush(container, 5000);
			}

			private long BlockChecksum(Dictionary<BlockId, BlockContainer> containersTouched, BlockId blockId) {
				// Fetch the block container,
				BlockContainer container = GetBlock(containersTouched, blockId);
				// Calculate the checksum value,
				return container.CreateChecksum();

			}

			private void UnbindWithManager() {
			}

			private void BindWithManager() {
			}
		}

		#endregion
	}
}