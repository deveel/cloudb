using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	static class ManagerCacheState {

		private readonly static Dictionary<IServiceAddress, INetworkCache> serviceCacheMap;
		private static readonly Dictionary<IServiceAddress, ServiceStatusTracker> trackerCacheMap;


		static ManagerCacheState() {
			serviceCacheMap = new Dictionary<IServiceAddress, INetworkCache>();
			trackerCacheMap = new Dictionary<IServiceAddress, ServiceStatusTracker>();
		}

		public static ServiceStatusTracker GetServiceTracker(IServiceAddress[] managers, IServiceConnector network) {
			lock (trackerCacheMap) {
				ServiceStatusTracker picked = null;
				int pickedCount = 0;
				for (int i = 0; i < managers.Length; ++i) {
					ServiceStatusTracker g;
					if (trackerCacheMap.TryGetValue(managers[i], out g)) {
						picked = g;
						++pickedCount;
					}
				}
				if (picked == null) {
					picked = new ServiceStatusTracker(network);
					for (int i = 0; i < managers.Length; ++i) {
						trackerCacheMap[managers[i]] = picked;
					}
				} else if (pickedCount != managers.Length) {
					for (int i = 0; i < managers.Length; ++i) {
						trackerCacheMap[managers[i]] = picked;
					}
				}
				return picked;
			}
		}

		private static INetworkCache CreateDefault() {
			return new MemoryNetworkCache(32 * 1024 * 1024);
		}

		public static INetworkCache GetCache(IServiceAddress[] managers) {
			lock (serviceCacheMap) {
				INetworkCache picked = null;
				int pickedCount = 0;
				for (int i = 0; i < managers.Length; ++i) {
					INetworkCache g;
					if (serviceCacheMap.TryGetValue(managers[i], out g)) {
						picked = g;
						++pickedCount;
					}
				}
				if (picked == null) {
					picked = CreateDefault();
					for (int i = 0; i < managers.Length; ++i) {
						serviceCacheMap[managers[i]] = picked;
					}
				} else if (pickedCount != managers.Length) {
					for (int i = 0; i < managers.Length; ++i) {
						serviceCacheMap[managers[i]] = picked;
					}
				}
				return picked;
			}

		}

		public static void SetCache(IServiceAddress[] manager, INetworkCache cache) {
			if (manager == null)
				throw new ArgumentNullException("manager");

			lock (serviceCacheMap) {
				for (int i = 0; i < manager.Length; i++) {
					serviceCacheMap[manager[i]] = cache;
				}
			}
		}
	}
}