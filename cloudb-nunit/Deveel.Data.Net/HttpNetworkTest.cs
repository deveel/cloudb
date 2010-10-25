using System;
using System.IO;
using System.Threading;

using Deveel.Data.Diagnostics;
using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(HttpMessageFormat.Xml, NetworkStoreType.Memory)]
	[TestFixture(HttpMessageFormat.Xml, NetworkStoreType.FileSystem)]
	[TestFixture(HttpMessageFormat.Json, NetworkStoreType.Memory)]
	[TestFixture(HttpMessageFormat.Json, NetworkStoreType.FileSystem)]
	public sealed class HttpNetworkTest {
		private readonly HttpMessageFormat format;
		private readonly NetworkStoreType storeType;
		
		private NetworkProfile networkProfile;
		private HttpAdminService adminService;
		private string path;

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);
				
		private static readonly HttpServiceAddress Local = new HttpServiceAddress("localhost", 1587);

		
		public HttpNetworkTest(HttpMessageFormat format, NetworkStoreType storeType) {
			this.format = format;
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
			
			config.SetValue(LogManager.NetworkLoggerName + "_type", "simple-console");
			LogManager.Init(config);
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), LogManager.NetworkLogger.BaseLogger);
		}
		
		[SetUp]
		public void SetUp() {
			SetupEvent.WaitOne();

			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(Local);
			netConfig.AddAllowedIp("localhost");
			netConfig.AddAllowedIp("127.0.0.1");
			Config(netConfig);

			IAdminServiceDelegator delegator = null;
			if (storeType == NetworkStoreType.Memory) {
				delegator = new MemoryAdminServiceDelegator();
			} else if (storeType == NetworkStoreType.FileSystem) {
				delegator = new FileAdminServiceDelegator(path);
			}

			adminService = new HttpAdminService(delegator, Local);
			adminService.Config = netConfig;
			adminService.Start();

			networkProfile = new NetworkProfile(new HttpServiceConnector("foo", "foo"));
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