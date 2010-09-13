using System;
using System.Net;

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
			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(FakeServiceAddress.Local);
			networkProfile.Configuration = netConfig;
		}
		
		[Test]
		public void StartManager() {
			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);	
			Assert.IsNull(networkProfile.ManagerServer);
			Assert.IsFalse(machine.IsManager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);
		}
		
		[Test]
		public void StartRoot() {
			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
		}
	}
}