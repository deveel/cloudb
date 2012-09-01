using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public static class MachineState {
		private static readonly Dictionary<IServiceAddress, INetworkCache> ServiceCacheMap;
		private static readonly Dictionary<IServiceAddress, ServiceStatusTracker> TrackerCacheMap;

		static MachineState() {
			ServiceCacheMap = new Dictionary<IServiceAddress, INetworkCache>();
			TrackerCacheMap = new Dictionary<IServiceAddress, ServiceStatusTracker>();
		}

		public static ServiceStatusTracker GetServiceTracker(IServiceAddress[] managers, IServiceConnector connector) {
			lock (TrackerCacheMap) {
				ServiceStatusTracker picked = null;
				int pickedCount = 0;
				for (int i = 0; i < managers.Length; ++i) {
					ServiceStatusTracker g;
					if (TrackerCacheMap.TryGetValue(managers[i], out g)) {
						picked = g;
						++pickedCount;
					}
				}
				if (picked == null) {
					picked = new ServiceStatusTracker(connector);
					for (int i = 0; i < managers.Length; ++i) {
						TrackerCacheMap[managers[i]] = picked;
					}
				} else if (pickedCount != managers.Length) {
					for (int i = 0; i < managers.Length; ++i) {
						TrackerCacheMap[managers[i]] = picked;
					}
				}
				return picked;
			}

		}

		public static INetworkCache GetCacheForManager(IServiceAddress[] managers) {
			lock (ServiceCacheMap) {
				INetworkCache picked = null;
				int pickedCount = 0;
				for (int i = 0; i < managers.Length; ++i) {
					INetworkCache g;
					if (ServiceCacheMap.TryGetValue(managers[i], out g)) {
						picked = g;
						++pickedCount;
					}
				}
				if (picked == null) {
					picked = CreateDefaultCacheFor(managers);
					for (int i = 0; i < managers.Length; ++i) {
						ServiceCacheMap[managers[i]] = picked;
					}
				} else if (pickedCount != managers.Length) {
					for (int i = 0; i < managers.Length; ++i) {
						ServiceCacheMap[managers[i]] = picked;
					}
				}
				return picked;
			}
		}

		private static INetworkCache CreateDefaultCacheFor(IServiceAddress[] managers) {
			return new InMemoryNetworkCache(32 * 1024 * 1024);
		}
	}
}