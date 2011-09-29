using System;
using System.Net;

using NUnit.Framework;

namespace Deveel.Data.Net.Security {
	[TestFixture]
	public sealed class TcpPlainAuthenticatorTest : AuthenticatorTestBase {
		public TcpPlainAuthenticatorTest(NetworkStoreType storeType) 
			: base(storeType) {
		}

		private static readonly TcpServiceAddress Local = new TcpServiceAddress(IPAddress.Loopback);

		protected override IServiceAddress LocalAddress {
			get { return Local; }
		}

		protected override AdminService CreateAdminService(NetworkStoreType storeType) {
			throw new NotImplementedException();
		}

		protected override IServiceConnector CreateConnector() {
			throw new NotImplementedException();
		}

		protected override IAuthenticator CreateAuthenticator() {
			throw new NotImplementedException();
		}
	}
}