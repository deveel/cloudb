using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class FakeAdminService : AdminService {
		private ConfigSource config;
		
		public FakeAdminService(FakeServiceConnector connector, FakeNetworkStoreType storeType)
			: base(FakeServiceAddress.Local, connector, new FakeAdminServiceDelegator(storeType)) {
		}
		
		public FakeAdminService(FakeServiceConnector connector)
			: this(connector, FakeNetworkStoreType.Memory) {
		}
		
		public FakeAdminService(FakeNetworkStoreType storeType)
			: this(null, storeType) {
			Connector = new FakeServiceConnector(ProcessCallback);
		}

		public FakeAdminService()
			: this(FakeNetworkStoreType.Memory) {
		}
		
		public ConfigSource Config {
			get { return config; }
			set { config = value; }
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
			private readonly FakeNetworkStoreType storeType;
			private string basePath;
			private IService manager;
			private IService root;
			private IService block;
			
			public FakeAdminServiceDelegator(FakeNetworkStoreType storeType) {
				this.storeType = storeType;
			}

			public void Dispose() {
			}

			public void Init(AdminService adminService) {
				if (storeType == FakeNetworkStoreType.FileSystem) {
					ConfigSource config = ((FakeAdminService)adminService).Config;
					basePath = config.GetString("node_directory", "./base");
				}
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
				if (storeType == FakeNetworkStoreType.Memory) {
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
				} else if (storeType == FakeNetworkStoreType.FileSystem) {
					if (serviceType == ServiceType.Manager) {
						string dbPath = Path.Combine(basePath, "manager");
						if (!Directory.Exists(dbPath))
							Directory.CreateDirectory(dbPath);
						
						manager = new FileSystemManagerService(connector, basePath, dbPath, address);
						return manager;
					}
					if (serviceType == ServiceType.Root) {
						string rootPath = Path.Combine(basePath, "root");
						if (!Directory.Exists(rootPath))
							Directory.CreateDirectory(rootPath);
						
						root = new FileSystemRootService(connector, rootPath);
						return root;
					}
					if (serviceType == ServiceType.Block) {
						string blockPath = Path.Combine(basePath, "block");
						if (!Directory.Exists(blockPath))
							Directory.CreateDirectory(blockPath);
						
						block = new FileSystemBlockService(connector, blockPath);
						return block;
					}
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