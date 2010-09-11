using System;

namespace Deveel.Data.Net {
	public class TcpNetworkClient : NetworkClient {
		public TcpNetworkClient(ServiceAddress managerAddress, string password) 
			: base(managerAddress, new TcpServiceConnector(password)) {
		}

		public TcpNetworkClient(ServiceAddress managerAddress, string password, INetworkCache cache) 
			: base(managerAddress, new TcpServiceConnector(password), cache) {
		}
	}
}
