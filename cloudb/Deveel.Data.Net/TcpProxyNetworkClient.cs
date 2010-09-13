using System;
using System.Net;

namespace Deveel.Data.Net {
	public class TcpProxyNetworkClient : NetworkClient {
		public TcpProxyNetworkClient(IPServiceAddress managerAddress, IPAddress proxyAddress, int proxyPort, string password) 
			: base(managerAddress, new TcpProxyServiceConnector(proxyAddress, proxyPort, password)) {
		}

		public TcpProxyNetworkClient(IPServiceAddress managerAddress, IPAddress proxyAddress, int proxyPort, string password, INetworkCache cache) 
			: base(managerAddress, new TcpProxyServiceConnector(proxyAddress, proxyPort, password), cache) {
		}
	}
}
