using System;
using System.Collections.Generic;
using System.Net;

using NUnit.Framework;

namespace Deveel.Data.Net.Security {
	[TestFixture]
	public sealed class NetworkAuthenticatorTest : AuthenticatorTestBase {
		public NetworkAuthenticatorTest(NetworkStoreType storeType) 
			: base(storeType) {
		}

		private const string Password = "abc$123456";

		private static readonly TcpServiceAddress Local = new TcpServiceAddress(IPAddress.Loopback);

		protected override IServiceAddress LocalAddress {
			get { return Local; }
		}

		protected override Type AuthenticatorType {
			get { return typeof (NetworkPasswordAuthenticator); }
		}

		protected override AdminService CreateAdminService(NetworkStoreType storeType) {
			throw new NotImplementedException();
		}

		protected override IServiceConnector CreateConnector() {
			throw new NotImplementedException();
		}

		protected override void PopulateAuthenticationData(IDictionary<string, AuthObject> authData) {
			throw new NotImplementedException();
		}
	}
}