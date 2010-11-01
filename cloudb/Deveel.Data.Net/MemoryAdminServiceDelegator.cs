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
		
		public void StartService(IServiceAddress address, ServiceType serviceType, IServiceConnector connector) {
			if (serviceType == ServiceType.Manager)
				manager = new MemoryManagerService(connector, address);
			if (serviceType == ServiceType.Root)
				root = new MemoryRootService(connector);
			if (serviceType == ServiceType.Block)
				block =  new MemoryBlockService(connector);
		}
		
		public void StopService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager && manager != null) {
				manager.Stop();
				manager = null;
			}
			if (serviceType == ServiceType.Root && root != null) {
				root.Stop();
				root = null;
			}
			if (serviceType == ServiceType.Block && block != null) {
				block.Stop();
				block = null;
			}
		}
		
		public void Dispose() {
			if (manager != null)
				manager.Dispose();
			if (root != null)
				root.Dispose();
			if (block != null)
				block.Dispose();
		}
	}
}