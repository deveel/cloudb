using System;

using Deveel.Data.Net;

using NUnit.Framework;

namespace Deveel.Data {
	public sealed class BasePathTest {
		private NetworkProfile networkProfile;
		private FakeAdminService adminService;

		private const string PathName = "testdb";

		[TestFixtureSetUp]
		public void TestFixtureSetUp() {
			adminService = new FakeAdminService();
			networkProfile = new NetworkProfile(new FakeServiceConnector(adminService));
			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(FakeServiceAddress.Local);
			networkProfile.Configuration = netConfig;


			// start a fake network to test in-memory ...
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Block);
			networkProfile.RegisterBlock(FakeServiceAddress.Local);
			networkProfile.Refresh();
		}

		[Test]
		public void TestAddPath() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, "Deveel.Data.BasePath, cloudbase");
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));
		}

		private static bool PathProfileExists(PathProfile profile) {
			if (profile.Path != PathName)
				return false;
			if (profile.PathType != typeof(BasePath).FullName)
				return false;
			return true;
		}
	}
}