using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class FakeAdminService : AdminService {		
		public FakeAdminService(FakeServiceConnector connector, NetworkStoreType storeType)
			: base(FakeServiceAddress.Local, connector, new FakeAdminServiceDelegator(storeType)) {
		}
		
		public FakeAdminService(FakeServiceConnector connector)
			: this(connector, NetworkStoreType.Memory) {
		}
		
		public FakeAdminService(NetworkStoreType storeType)
			: this(null, storeType) {
			Connector = new FakeServiceConnector(ProcessCallback);
		}

		public FakeAdminService()
			: this(NetworkStoreType.Memory) {
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
			private readonly NetworkStoreType storeType;
			private string basePath;
			private IService manager;
			private IService root;
			private IService block;
			
			public FakeAdminServiceDelegator(NetworkStoreType storeType) {
				this.storeType = storeType;
			}

			public void Dispose() {
				if (manager != null)
					manager.Dispose();
				if (root != null)
					root.Dispose();
				if (block != null)
					block.Dispose();
			}

			public void Init(AdminService adminService) {
				if (storeType == NetworkStoreType.FileSystem) {
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
				if (storeType == NetworkStoreType.Memory) {
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
				} else if (storeType == NetworkStoreType.FileSystem) {
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