using System;

namespace Deveel.Data.Net {
	public sealed class MemoryAdminServiceDelegator : IAdminServiceDelegator {
		public void Init(AdminService adminService) {
		}
		
		private IService manager;
		private IService root;
		private IService block;
		
		public IService GetService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager)
				return manager;
			if (serviceType == ServiceType.Root)
				return root;
			if (serviceType == ServiceType.Block)
				return block;
			
			throw new ArgumentException("Invalid service type specified.");
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
		}
		
		public void Dispose() {
		}
	}
}