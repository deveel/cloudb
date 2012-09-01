using System;
using System.Collections.Generic;

using Deveel.Data.Caching;

namespace Deveel.Data.Net {
	public class InMemoryNetworkCache : INetworkCache {
		private readonly LocalCache heapCache;
		private readonly Dictionary<BlockId, BlockCacheElement> s2BlockCache;
		private readonly Dictionary<String, PathInfo> pathInfoMap;

		public InMemoryNetworkCache(long maxCacheSize) {
			heapCache = new LocalCache(Cache.ClosestPrime(12000), maxCacheSize);
			s2BlockCache = new Dictionary<BlockId, BlockCacheElement>(1023);
			pathInfoMap = new Dictionary<string, PathInfo>(255);
		}

		public InMemoryNetworkCache()
			: this(32 * 1024 * 1024) {
		}


		public void SetNode(DataAddress address, ITreeNode node) {
			lock (heapCache) {
				heapCache.Set(address, node);
			}
		}

		public ITreeNode GetNode(DataAddress address) {
			lock (heapCache) {
				return heapCache.Get(address) as ITreeNode;
			}
		}

		public void RemoveNode(DataAddress address) {
			lock (heapCache) {
				heapCache.Remove(address);
			}
		}

		public PathInfo GetPathInfo(string pathName) {
			lock (pathInfoMap) {
				PathInfo pathInfo;
				if (!pathInfoMap.TryGetValue(pathName, out pathInfo))
					return null;

				return pathInfo;
			}
		}

		public void SetPathInfo(string pathName, PathInfo pathInfo) {
			lock (pathInfoMap) {
				pathInfoMap[pathName] = pathInfo;
			}
		}

		public IList<BlockServerElement> GetServersWithBlock(BlockId blockId) {
			lock (s2BlockCache) {
				BlockCacheElement ele;
				if (!s2BlockCache.TryGetValue(blockId, out ele) || 
					DateTime.Now > ele.TimeToEnd) {
					return null;
				}
				return ele.BlockServers;
			}
		}

		public void SetServersForBlock(BlockId blockId, IList<BlockServerElement> servers, int ttlHint) {
			BlockCacheElement ele = new BlockCacheElement();
			ele.BlockServers = servers;
			ele.TimeToEnd = DateTime.Now.AddMilliseconds(ttlHint);

			lock (s2BlockCache) {
				s2BlockCache[blockId] = ele;
			}
		}

		public void RemoveServersWithBlock(BlockId blockId) {
			lock (s2BlockCache) {
				s2BlockCache.Remove(blockId);
			}
		}

		#region BlockCacheElement

		class BlockCacheElement {
			public DateTime TimeToEnd;
			public IList<BlockServerElement> BlockServers; 
		}

		#endregion

		#region LocalCache

		private class LocalCache : MemoryCache {
			private long sizeEstimate;

			private readonly long maxCacheSize;
			private readonly long cleanTo;

			public LocalCache(int size, long maxCacheSize)
				: base(size) {
				sizeEstimate = 0;
				this.maxCacheSize = maxCacheSize;
				cleanTo = (long) ((double) maxCacheSize*(double) .75);
			}

			protected override void CheckClean() {
				// If we have reached maximum cache size, remove some elements from the
				// end of the list
				if (sizeEstimate >= maxCacheSize) {
					Clean();
				}
			}

			protected override bool WipeMoreNodes() {
				return sizeEstimate >= cleanTo;
			}

			protected override void OnObjectAdded(object key, object val) {
				sizeEstimate += ((ITreeNode)val).MemoryAmount + 64;
			}

			protected override void OnObjectRemoved(object key, object val) {
				sizeEstimate -= ((ITreeNode)val).MemoryAmount + 64;
			}

			protected override void OnAllCleared() {
				sizeEstimate = 0;
			}
		}

		#endregion
	}
}