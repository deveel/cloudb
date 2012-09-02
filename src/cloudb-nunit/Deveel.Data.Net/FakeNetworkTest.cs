using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(StoreType.Memory)]
	[TestFixture(StoreType.FileSystem)]
	public class FakeNetworkTest : NetworkServiceTestBase {
		public FakeNetworkTest(StoreType storeType)
			: base(storeType) {
		}

		protected override IServiceAddress LocalAddress {
			get { return FakeServiceAddress.Local; }
		}

		protected override AdminService CreateAdminService(StoreType storeType) {
			return new FakeAdminService(storeType);
		}

		protected override IServiceConnector CreateConnector() {
			return new FakeServiceConnector((FakeAdminService) AdminService);
		}
	}
}