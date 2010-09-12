using System;
using System.Net;

namespace Deveel.Data.Net {
	public sealed class TcpMemoryAdminService : TcpAdminService {
		public TcpMemoryAdminService(NetworkConfigSource config, IPAddress address, int port, string password) 
			: base(config, address, port, password) {
		}

		protected override IService CreateService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager)
				return new MemoryManagerService(Connector, Address);
			if (serviceType == ServiceType.Root)
				return new MemoryRootService(Connector);
			if (serviceType == ServiceType.Block)
				return new MemoryBlockService(Connector);

			throw new InvalidOperationException();
		}

		protected override void DisposeService(IService service) {
			service.Dispose();
		}
	}
}