using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory)]
	[TestFixture(NetworkStoreType.FileSystem)]
	public class FakeNetworkTest : NetworkTestBase {		
		public FakeNetworkTest(NetworkStoreType storeType)
			: base(storeType) {
		}

		protected override IServiceAddress[] LocalAddresses {
			get { return new IServiceAddress[] {FakeServiceAddress.Local1}; }
		}

		protected override AdminService CreateAdminService(NetworkStoreType storeType) {
			return new FakeAdminService(storeType);
		}

		protected override IServiceConnector CreateConnector() {
			return new FakeServiceConnector((FakeAdminService) AdminService);
		}
	}
}