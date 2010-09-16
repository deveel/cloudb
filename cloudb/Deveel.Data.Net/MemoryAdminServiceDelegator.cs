using System;

namespace Deveel.Data.Net {
	public sealed class MemoryAdminServiceDelegator : IAdminServiceDelegator {
		public void Init(AdminService adminService) {
		}
		
		public IService GetService(ServiceType serviceType) {
			throw new NotImplementedException();
		}
		
		public IService CreateService(IServiceAddress address, ServiceType serviceType, IServiceConnector connector) {
			if (serviceType == ServiceType.Manager)
				return new MemoryManagerService(connector, address);
			if (serviceType == ServiceType.Root)
				return new MemoryRootService(connector);
			if (serviceType == ServiceType.Block)
				return new MemoryBlockService(connector);

			throw new InvalidOperationException();
		}
		
		public void DisposeService(ServiceType serviceType) {
			throw new NotImplementedException();
		}
		
		public void Dispose()
		{
			throw new NotImplementedException();
		}
	}
}