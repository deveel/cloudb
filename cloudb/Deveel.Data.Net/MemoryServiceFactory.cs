using System;

namespace Deveel.Data.Net {
	public sealed class MemoryServiceFactory : IServiceFactory {
		public void Init(AdminService adminService) {
		}

		public IService CreateService(IServiceAddress serviceAddress, ServiceType serviceType, IServiceConnector connector) {
			if (serviceType == ServiceType.Manager)
				return new MemoryManagerService(connector, serviceAddress);
			if (serviceType == ServiceType.Root)
				return new MemoryRootService(connector, serviceAddress);
			if (serviceType == ServiceType.Block)
				return new MemoryBlockService(connector);

			throw new ArgumentException("An invalid service type was specified.");
		}
	}
}