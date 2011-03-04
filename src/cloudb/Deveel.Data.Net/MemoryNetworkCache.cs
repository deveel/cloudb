using System;
using System.Collections.Generic;

using Deveel.Data.Caching;

namespace Deveel.Data.Net {
	public class MemoryNetworkCache : INetworkCache {
		public MemoryNetworkCache(long max_cache_size) {
			heap_cache = new LocalCache(Cache.ClosestPrime(4096), max_cache_size);
			s2block_cache = new Dictionary<BlockId, BlockCacheElement>(1024);
			pathInfoMap = new Dictionary<string, PathInfo>(255);
		}

		public MemoryNetworkCache()
			: this(32 * 1024 * 1024) {
		}

		private readonly LocalCache heap_cache;
		private readonly Dictionary<BlockId, BlockCacheElement> s2block_cache;
		private readonly Dictionary<string, PathInfo> pathInfoMap;

		#region Implementation of ITreeNodeCache

		public void SetNode(DataAddress address, ITreeNode node) {
			lock (heap_cache) {
				heap_cache.Set(address, node);
			}
		}

		public ITreeNode GetNode(DataAddress address) {
			lock (heap_cache) {
				return (ITreeNode)heap_cache.Get(address);
			}
		}

		public void RemoveNode(DataAddress address) {
			lock (heap_cache) {
				heap_cache.Remove(address);
			}
		}

		#endregion

		#region Implementation of INetworkCache

		public void SetServers(BlockId blockId, IList<BlockServerElement> servers, int ttlHint) {
			BlockCacheElement ele = new BlockCacheElement();
			ele.block_servers = servers;
			ele.time_to_end = DateTime.Now.AddMilliseconds(ttlHint);

			lock (s2block_cache) {
				s2block_cache[blockId] = ele;
			}
		}

		public IList<BlockServerElement> GetServers(BlockId blockId) {
			lock (s2block_cache) {
				BlockCacheElement ele;
				if (!s2block_cache.TryGetValue(blockId, out ele) ||
					DateTime.Now > ele.time_to_end)
					return null;
				return ele.block_servers;
			}
		}

		public void RemoveServers(BlockId blockId) {
			lock (s2block_cache) {
				s2block_cache.Remove(blockId);
			}
		}

		public PathInfo GetPathInfo(string pathName) {
			lock (pathInfoMap) {
				PathInfo pathInfo;
				if (pathInfoMap.TryGetValue(pathName, out pathInfo))
					return pathInfo;
				return null;
			}
		}

		public void SetPathInfo(string pathName, PathInfo pathInfo) {
			lock (pathInfoMap) {
				pathInfoMap[pathName] = pathInfo;
			}
		}

		#endregion

		#region LocalCache

		private class LocalCache : MemoryCache {
			internal LocalCache(int size, long max_cache_size)
				: base(size, 20) {
				size_estimate = 0;
				this.max_cache_size = max_cache_size;
				clean_to = (long)(max_cache_size * .75);
			}

			private long size_estimate;
			private readonly long max_cache_size;
			private readonly long clean_to;

			protected override void CheckClean() {
				// If we have reached maximum cache size, remove some elements from the
				// end of the list
				if (size_estimate >= max_cache_size)
					Clean();
			}

			protected override bool WipeMoreNodes() {
				return size_estimate >= clean_to;
			}

			protected override bool SetObject(object key, object value) {
				bool b = base.SetObject(key, value);
				size_estimate += ((ITreeNode)value).MemoryAmount + 64;
				return b;
			}

			protected override object RemoveObject(object key) {
				object obj = base.RemoveObject(key);
				size_estimate -= ((ITreeNode)obj).MemoryAmount + 64;
				return obj;
			}

			public override void Clear() {
				base.Clear();
				size_estimate = 0;
			}
		}

		#endregion

		#region BlockCacheElement

		private sealed class BlockCacheElement {
			public DateTime time_to_end;
			public IList<BlockServerElement> block_servers;
		}

		#endregion
	}
}