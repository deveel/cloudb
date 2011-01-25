using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	static class ManagerCacheState {

		private readonly static Dictionary<IServiceAddress, INetworkCache> serviceCacheMap;

		static ManagerCacheState() {
			serviceCacheMap = new Dictionary<IServiceAddress, INetworkCache>();
		}

		private static INetworkCache CreateDefault() {
			return new MemoryNetworkCache(32 * 1024 * 1024);
		}

		public static INetworkCache GetCache(IServiceAddress manager) {
			lock (serviceCacheMap) {
				INetworkCache cache;
				if (!serviceCacheMap.TryGetValue(manager, out cache)) {
					cache = CreateDefault();
					serviceCacheMap[manager] = cache;
				}
				return cache;
			}
		}

		public static void SetCache(IServiceAddress manager, INetworkCache cache) {
			if (manager == null)
				throw new ArgumentNullException("manager");

			lock (serviceCacheMap) {
				serviceCacheMap[manager] = cache;
			}
		}
	}
}