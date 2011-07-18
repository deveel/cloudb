using System;

using Deveel.Data.Configuration;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class FakeAdminService : AdminService {		
		public FakeAdminService(FakeServiceConnector connector, NetworkStoreType storeType)
			: base(FakeServiceAddress.Local, connector, new FakeServiceFactory(storeType)) {
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
		
		internal Message ProcessCallback(ServiceType serviceType, Message inputStream) {
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

		#region FakeServiceFactory

		private class FakeServiceFactory : IServiceFactory {
			private IServiceFactory factory;
			private readonly NetworkStoreType storeType;

			public FakeServiceFactory(NetworkStoreType storeType) {
				this.storeType = storeType;
			}

			public void Init(AdminService adminService) {
				if (storeType == NetworkStoreType.FileSystem) {
					ConfigSource config = adminService.Config;
					string basePath = config.GetString("node_directory", "./base");
					factory = new FileSystemServiceFactory(basePath);
				} else {
					factory = new MemoryServiceFactory();
				}

				factory.Init(adminService);
			}

			public IService CreateService(IServiceAddress serviceAddress, ServiceType serviceType, IServiceConnector connector) {
				return factory.CreateService(serviceAddress, serviceType, connector);
			}
		}

		#endregion
	}
}