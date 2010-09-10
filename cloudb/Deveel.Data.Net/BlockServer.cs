using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public abstract class BlockServer : IService {
		private long serverGuid;
		private volatile int blockCount = 0;
		private long lastBlockId = -1;
		private bool disposed;
		private readonly IServiceConnector connector;
		private ErrorStateException errorState;
		
		private readonly Dictionary<long, BlockContainer> blockContainerCache;
		private readonly LinkedList<BlockContainer> blockContainerAccessList;
		private readonly object pathLock = new object();
		
		protected BlockServer(IServiceConnector connector) {
			this.connector = connector;
			
			blockContainerCache = new Dictionary<long, BlockContainer>(5279);
			blockContainerAccessList = new LinkedList<BlockContainer>();
		}
		
		public ServiceType ServiceType {
			get { return ServiceType.Block; }
		}
		
		public IMessageProcessor Processor {
			get { throw new NotImplementedException(); }
		}
		
		private void Dispose(bool disposing) {
			if (!disposed) {
				OnDispose(disposing);
				
				if (disposing) {
					lock (pathLock) {
						blockContainerCache.Clear();
						blockContainerAccessList.Clear();
					}
				
					blockCount = 0;
					lastBlockId = -1;
				}
				
				//TODO:
				disposed = true;
			}
		}
		
		private void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}
		
		private void SetErrorState(Exception e) {
			errorState = new ErrorStateException(e);
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
		
		protected void SetGuid(long value) {
			serverGuid = value;
		}
		
		protected virtual void OnInit() {
		}
		
		protected virtual void OnDispose(bool disposing) {
		}
		
		protected abstract BlockContainer LoadBlock(long blockId);
		
		protected abstract long[] ListBlocks();
		
		public void Init() {
			OnInit();
			
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
			// The latest block on this server,
			this.lastBlockId = inLastBlockId;
		}
		
		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		
		#region BlockServerMessageProcessor
		
		private sealed class BlockServerMessageProcessor : IMessageProcessor {
			private readonly BlockServer server;
			
			public BlockServerMessageProcessor(BlockServer server) {
				this.server = server;
			}
			
			private void CloseContainers(Dictionary<long, BlockContainer> touched) {
				foreach(KeyValuePair<long, BlockContainer> c in touched) {
					//TODO: DEBUG log ..
					c.Value.Close();
				}
			}
			
			private BlockContainer getBlock(IDictionary<long, BlockContainer> touched, long blockId) {
				BlockContainer b;
				if (!touched.TryGetValue(blockId, out b)) {
					b = server.FetchBlockContainer(blockId);
					bool created = b.Open();
					if (created) {
						++server.blockCount;
						server.lastBlockId = blockId;
					}
					touched[blockId] = b;
					//TODO: DEBUG log ...
				}
				return b;
			}
			
			public MessageStream Process(MessageStream messageStream) {
				// The map of containers touched,
				Dictionary<long, BlockContainer> containersTouched = new Dictionary<long, BlockContainer>();
				// The nodes fetched in this message,
				List<long> readNodes = null;
				
				MessageStream responseStream = new MessageStream(32);
				
				foreach(Message m in messageStream) {
					try {
						server.CheckErrorState();
						
						switch(m.Name) {
							case "bindWithManager":
							case "unbindWithManager":
								//TODO: is this needed for a block server?
								responseStream.AddMessage("R", 1);
								break;
							case "serverGUID":
								responseStream.AddMessage("R", server.serverGuid);
								break;
							default:
								throw new ApplicationException("Unknown message name: " + m.Name);
						}
					} catch(OutOfMemoryException e) {
						//TODO: ERROR log ...
						server.SetErrorState(e);
						throw;
					} catch (Exception e) {
						//TODO: ERROR log ...
						responseStream.AddErrorMessage(new ServiceException(e));
					}
				}
				
				// Release any containers touched,
				try {
					CloseContainers(containersTouched);
				} catch (Exception e) {
					//TODO: ERROR log ...
				}
				
				return responseStream;
			}
		}
		
		#endregion
		
		#region BlockContainer
		
		protected sealed class BlockContainer : IBlockStore, IComparable<BlockContainer> {
			public IBlockStore blockStore;
			public readonly long blockId;
			private DateTime lastWrite = DateTime.MinValue;
			private int lockCount = 0;
			
			internal BlockContainer(long blockId, IBlockStore blockStore) {
				this.blockId = blockId;
				this.blockStore = blockStore;
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