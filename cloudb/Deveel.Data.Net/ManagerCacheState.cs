using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	static class ManagerCacheState {

		private readonly static Dictionary<ServiceAddress, INetworkCache> serviceCacheMap;

		static ManagerCacheState() {
			serviceCacheMap = new Dictionary<ServiceAddress, INetworkCache>();
		}

		private static INetworkCache CreateDefault() {
			return new MemoryNetworkCache(32 * 1024 * 1024);
		}

		public static INetworkCache GetCache(ServiceAddress manager) {
			lock (serviceCacheMap) {
				INetworkCache cache;
				if (!serviceCacheMap.TryGetValue(manager, out cache)) {
					cache = CreateDefault();
					serviceCacheMap[manager] = cache;
				}
				return cache;
			}
		}

		public static void SetCache(ServiceAddress manager, INetworkCache cache) {
			if (manager == null)
				throw new ArgumentNullException("manager");

			lock (serviceCacheMap) {
				serviceCacheMap[manager] = cache;
			}
		}
	}
}