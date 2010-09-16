using System;

namespace Deveel.Data.Net {
	public sealed class FakeAdminService : AdminService {
		public FakeAdminService(FakeServiceConnector connector)
			: base(FakeServiceAddress.Local, connector, new FakeAdminServiceDelegator()) {
		}

		public FakeAdminService()
			: this(null) {
			Connector = new FakeServiceConnector(ProcessCallback);
		}

		internal MessageStream ProcessCallback(ServiceType serviceType, MessageStream inputStream) {
			if (serviceType == ServiceType.Admin)
				return Processor.Process(inputStream);
			if (serviceType == ServiceType.Manager)
				return Manager.Processor.Process(inputStream);
			if (serviceType == ServiceType.Root)
				return Root.Processor.Process(inputStream);
			if (serviceType == ServiceType.Block)
				return Block.Processor.Process(inputStream);

			throw new InvalidOperationException();
		}

		#region FakeAdminServiceDelegator

		private class FakeAdminServiceDelegator : IAdminServiceDelegator {
			private IService manager;
			private IService root;
			private IService block;

			public void Dispose() {
			}

			public void Init(AdminService adminService) {
				throw new NotImplementedException();
			}

			public IService GetService(ServiceType serviceType) {
				if (serviceType == ServiceType.Manager)
					return manager;
				if (serviceType == ServiceType.Root)
					return root;
				if (serviceType == ServiceType.Block)
					return block;

				throw new ArgumentException();
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
					block = new MemoryBlockService(connector);
					return block;
				}

				throw new InvalidOperationException();
			}

			public void DisposeService(ServiceType serviceType) {
				if (serviceType == ServiceType.Manager) {
					manager.Dispose();
					manager = null;
				} else if (serviceType == ServiceType.Root) {
					root.Dispose();
					root = null;
				} else if (serviceType == ServiceType.Block) {
					block.Dispose();
					block = null;
				}
			}
		}
		#endregion

	}
}