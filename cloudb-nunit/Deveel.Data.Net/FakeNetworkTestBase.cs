using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public class FakeNetworkTestBase {
		private NetworkProfile networkProfile;
		private FakeAdminService adminService;

		[TestFixtureSetUp]
		public void SetUp() {
			adminService = new FakeAdminService();
			networkProfile = new NetworkProfile(new FakeServiceConnector(adminService));
		}
	}
}