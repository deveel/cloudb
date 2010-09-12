using System;
using System.Net;

namespace Deveel.Data.Net {
	public sealed class FakeAdminService : AdminService {
		private readonly FakeServiceConnector connector;

		public FakeAdminService(FakeServiceConnector connector) {
			this.connector = connector;
		}

		public FakeAdminService() {
			connector = new FakeServiceConnector(ProcessCallback);
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

		protected override IService CreateService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager)
				return new MemoryManagerService(connector, new ServiceAddress(IPAddress.Loopback, 1212));
			if (serviceType == ServiceType.Root)
				return new MemoryRootService(connector);
			if (serviceType == ServiceType.Block)
				return new MemoryBlockService(connector);

			throw new InvalidOperationException();
		}

		protected override void DisposeService(IService service) {
			service.Dispose();
		}
	}
}