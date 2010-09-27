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
			if (serviceType == ServiceType.Manager) {
				manager = new MemoryManagerService(connector, address);
				return manager;
			}
			if (serviceType == ServiceType.Root) {
				root = new MemoryRootService(connector);
				return root;
			}
			if (serviceType == ServiceType.Block) {
				block =  new MemoryBlockService(connector);
				return block;
			}

			throw new InvalidOperationException();
		}
		
		public void DisposeService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager && manager != null) {
				manager.Dispose();
				manager = null;
			}
			if (serviceType == ServiceType.Root && root != null) {
				root.Dispose();
				root = null;
			}
			if (serviceType == ServiceType.Block && block != null) {
				block.Dispose();
				block = null;
			}
		}
		
		public void Dispose() {
			DisposeService(ServiceType.Manager);
			DisposeService(ServiceType.Root);
			DisposeService(ServiceType.Block);
		}
	}
}