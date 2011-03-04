using System;

namespace Deveel.Data.Net {
	public class TcpNetworkClient : NetworkClient {
		public TcpNetworkClient(TcpServiceAddress[] managerAddresses, string password)
			: base(managerAddresses, new TcpServiceConnector(password)) {	
		}

		public TcpNetworkClient(TcpServiceAddress[] managerAddresses, string password, INetworkCache cache)
			: base(managerAddresses, new TcpServiceConnector(password), cache) {
		}

		public TcpNetworkClient(TcpServiceAddress managerAddress, string password) 
			: this(new TcpServiceAddress[] { managerAddress }, password) {
		}

		public TcpNetworkClient(TcpServiceAddress managerAddress, string password, INetworkCache cache) 
			: this(new TcpServiceAddress[] { managerAddress }, password, cache) {
		}
	}
}
