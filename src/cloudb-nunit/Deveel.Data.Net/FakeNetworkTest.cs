<<<<<<< HEAD
﻿using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory)]
	[TestFixture(NetworkStoreType.FileSystem)]
	public class FakeNetworkTest : NetworkTestBase {		
		public FakeNetworkTest(NetworkStoreType storeType)
			: base(storeType) {
		}

		protected override IServiceAddress LocalAddress {
			get { return FakeServiceAddress.Local; }
		}

		protected override AdminService CreateAdminService(NetworkStoreType storeType) {
			return new FakeAdminService(storeType);
		}

		protected override IServiceConnector CreateConnector() {
			return new FakeServiceConnector((FakeAdminService) AdminService);
		}
	}
=======
﻿using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory)]
	[TestFixture(NetworkStoreType.FileSystem)]
	public class FakeNetworkTest : NetworkServiceTestBase {
		public FakeNetworkTest(NetworkStoreType storeType)
			: base(storeType) {
		}

		protected override IServiceAddress LocalAddress {
			get { return FakeServiceAddress.Local; }
		}

		protected override AdminService CreateAdminService(NetworkStoreType storeType) {
			return new FakeAdminService(storeType);
		}

		protected override IServiceConnector CreateConnector() {
			return new FakeServiceConnector((FakeAdminService) AdminService);
		}
	}
>>>>>>> master
}