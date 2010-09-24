using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	public abstract class FakeNetworkTest {
		private NetworkProfile networkProfile;
		private FakeAdminService adminService;
		
		protected abstract FakeNetworkStoreType StoreType { get; }
		
		protected virtual void Config(ConfigSource config) {
		}
		
		protected virtual void OnSetUp() {
		}
		
		protected virtual void OnTearDown() {
		}
		
		[SetUp]
		public void SetUp() {
			adminService = new FakeAdminService(StoreType);
			ConfigSource config = new ConfigSource();
			Config(config);
			adminService.Config = config;
			adminService.Init();
			networkProfile = new NetworkProfile(new FakeServiceConnector(adminService));
			
			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(FakeServiceAddress.Local);
			networkProfile.Configuration = netConfig;
			
			OnSetUp();
		}
		
		[TearDown]
		public void TearDown() {
			adminService.Dispose();
			
			OnTearDown();
		}
		
		[Test]
		public void Test1_StartManager() {
			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);	
			Assert.IsNull(networkProfile.ManagerServer);
			Assert.IsFalse(machine.IsManager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsNotNull(networkProfile.ManagerServer);
			Assert.IsTrue(machine.IsManager);
		}
		
		[Test]
		public void Test1_StartRoot() {
			Test1_StartManager();
			
			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);
		}

		[Test]
		public void Test1_StartBlock() {
			Test1_StartManager();
			
			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Block);
			networkProfile.RegisterBlock(FakeServiceAddress.Local);
		}


		[Test]
		public void Test1_StartAllServices() {
			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsNull(networkProfile.ManagerServer);
			Assert.IsFalse(machine.IsManager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);

			networkProfile.Refresh();
			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsManager);

			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);

			networkProfile.Refresh();
			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsRoot);

			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Block);
			networkProfile.RegisterBlock(FakeServiceAddress.Local);

			networkProfile.Refresh();
			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsBlock);
		}

		[Test]
		public void StartAndStopManager() {
			Test1_StartManager();

			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsManager);

			networkProfile.StopService(FakeServiceAddress.Local, ServiceType.Manager);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsManager);

		}

		[Test]
		public void StartAndStopRoot() {
			Test1_StartRoot();

			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsRoot);

			networkProfile.StopService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
		}

		[Test]
		public void StartAndStopBlock() {
			Test1_StartBlock();

			MachineProfile machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsBlock);

			networkProfile.StopService(FakeServiceAddress.Local, ServiceType.Block);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(FakeServiceAddress.Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
		}

		[Test]
		public void StartAndStopAllServices() {
			//TODO:
		}
	}
}