using System;

using Deveel.Data.Net.Security;

namespace Deveel.Data.Net {
	public class TcpNetworkClient : NetworkClient {
		public TcpNetworkClient(TcpServiceAddress managerAddress, IServiceAuthenticator authenticator) 
			: base(managerAddress, new TcpServiceConnector(authenticator)) {
		}

		public TcpNetworkClient(TcpServiceAddress managerAddress, string password)
			: this(managerAddress, new NetworkPasswordAuthenticator(password)) {
		}

		public TcpNetworkClient(TcpServiceAddress managerAddress, IServiceAuthenticator authenticator, INetworkCache cache) 
			: base(managerAddress, new TcpServiceConnector(authenticator), cache) {
		}

		public TcpNetworkClient(TcpServiceAddress managerAddress, string password, INetworkCache cache)
			: this(managerAddress, new NetworkPasswordAuthenticator(password), cache) {
		}
	}
}
