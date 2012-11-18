using System;

namespace Deveel.Data.Net {
	public class TcpProxyNetworkClient : NetworkClient {
		public TcpProxyNetworkClient(TcpServiceAddress managerAddress, TcpServiceAddress proxyAddress, string password) 
			: base(managerAddress, new TcpProxyServiceConnector(proxyAddress, password)) {
		}

		public TcpProxyNetworkClient(TcpServiceAddress managerAddress, TcpServiceAddress proxyAddress, string password, INetworkCache cache) 
			: base(managerAddress, new TcpProxyServiceConnector(proxyAddress, password), cache) {
		}
	}
}
