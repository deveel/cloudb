using System;
using System.IO;
using System.Net;
using System.Threading;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.FileSystem)]
	[TestFixture(NetworkStoreType.Memory)]
	public sealed class TcpNetworkTest {
		private readonly NetworkStoreType storeType;
		private NetworkProfile networkProfile;
		private TcpAdminService adminService;
		private string path;

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);
		
		private const string NetworkPassword = "123456";
		
		private static readonly TcpServiceAddress Local = new TcpServiceAddress(IPAddress.Loopback);

		
		public TcpNetworkTest(NetworkStoreType storeType) {
			this.storeType = storeType;
		}
		
		private void Config(ConfigSource config) {
			if (storeType == NetworkStoreType.FileSystem) {
				path = Path.Combine(Environment.CurrentDirectory, "base");
				if (Directory.Exists(path))
					Directory.Delete(path, true);
			
				Directory.CreateDirectory(path);
			
				config.SetValue("node_directory", path);
			}
		}


		[SetUp]
		public void SetUp() {
			SetupEvent.WaitOne();

			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(Local);
			Config(netConfig);

			IAdminServiceDelegator delegator = null;
			if (storeType == NetworkStoreType.Memory) {
				delegator = new MemoryAdminServiceDelegator();
			} else if (storeType == NetworkStoreType.FileSystem) {
				delegator = new FileAdminServiceDelegator(path);
			}

			adminService = new TcpAdminService(delegator, Local, NetworkPassword);
			adminService.Config = netConfig;
			adminService.Init();

			networkProfile = new NetworkProfile(new TcpServiceConnector(NetworkPassword));
			networkProfile.Configuration = netConfig;

			SetupEvent.Set();
		}

		[TearDown]
		public void TearDown() {
			adminService.Dispose();

			if (storeType == NetworkStoreType.FileSystem &&
			    Directory.Exists(path))
				Directory.Delete(path, true);

			SetupEvent.Set();
		}

		[Test]
		public void Test1_StartManager() {
			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);	
			Assert.IsNull(networkProfile.ManagerServer);
			Assert.IsFalse(machine.IsManager);
			networkProfile.StartService(Local, ServiceType.Manager);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsNotNull(networkProfile.ManagerServer);
			Assert.IsTrue(machine.IsManager);
		}
		
		[Test]
		public void Test1_StartRoot() {
			Test1_StartManager();
			
			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			networkProfile.StartService(Local, ServiceType.Root);
			networkProfile.RegisterRoot(Local);
		}

		[Test]
		public void Test1_StartBlock() {
			Test1_StartManager();
			
			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
			networkProfile.StartService(Local, ServiceType.Block);
			networkProfile.RegisterBlock(Local);
		}


		[Test]
		public void Test1_StartAllServices() {
			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsNull(networkProfile.ManagerServer);
			Assert.IsFalse(machine.IsManager);
			networkProfile.StartService(Local, ServiceType.Manager);

			networkProfile.Refresh();
			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsManager);

			networkProfile.StartService(Local, ServiceType.Root);
			networkProfile.RegisterRoot(Local);

			networkProfile.Refresh();
			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsRoot);

			networkProfile.StartService(Local, ServiceType.Block);
			networkProfile.RegisterBlock(Local);

			networkProfile.Refresh();
			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsBlock);
		}

		[Test]
		public void StartAndStopManager() {
			Test1_StartManager();

			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsManager);

			networkProfile.StopService(Local, ServiceType.Manager);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsManager);

		}

		[Test]
		public void StartAndStopRoot() {
			Test1_StartRoot();

			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsRoot);

			networkProfile.StopService(Local, ServiceType.Root);
			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
		}

		[Test]
		public void StartAndStopBlock() {
			Test1_StartBlock();

			MachineProfile machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsBlock);

			networkProfile.StopService(Local, ServiceType.Block);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(Local);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
		}

		[Test]
		public void StartAndStopAllServices() {
			Test1_StartAllServices();

			//TODO:
		}
	}
}