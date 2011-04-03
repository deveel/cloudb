using System;
using System.IO;
using System.Threading;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public abstract class NetworkTestBase {
		private NetworkProfile networkProfile;
		private AdminService adminService;
		private readonly NetworkStoreType storeType;
		private string path;

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);

		protected NetworkTestBase(NetworkStoreType storeType) {
			this.storeType = storeType;
		}

		protected AdminService AdminService {
			get { return adminService; }
		}

		protected string TestPath {
			get {
				if (path == null)
					path = Path.Combine(Environment.CurrentDirectory, "base");
				return path;
			}
		}

		protected abstract IServiceAddress[] LocalAddresses { get; }

		protected virtual void Config(ConfigSource config) {
			if (storeType == NetworkStoreType.FileSystem) {
				if (Directory.Exists(TestPath))
					Directory.Delete(TestPath, true);

				Directory.CreateDirectory(path);

				config.SetValue("node_directory", path);
			}

			config.SetValue("logger." + Logger.NetworkLoggerName + ".type", "simple-console");
			Logger.Init(config);
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), Logger.Network.BaseLogger);
		}

		protected abstract AdminService CreateAdminService(NetworkStoreType storeType);

		protected abstract IServiceConnector CreateConnector();

		[SetUp]
		public void SetUp() {
			SetupEvent.WaitOne();

			adminService = CreateAdminService(storeType);
			NetworkConfigSource config = new NetworkConfigSource();
			Config(config);
			adminService.Config = config;
			adminService.Start();
			networkProfile = new NetworkProfile(CreateConnector());

			NetworkConfigSource netConfig = new NetworkConfigSource();
			for (int i = 0; i < LocalAddresses.Length; i++) {
				netConfig.AddNetworkNode(LocalAddresses[i]);
			}
			networkProfile.Configuration = netConfig;

			SetupEvent.Set();
		}

		[TearDown]
		public void TearDown() {
			try {
				adminService.Stop();
				adminService.Dispose();

				if (storeType == NetworkStoreType.FileSystem &&
					Directory.Exists(TestPath))
					Directory.Delete(TestPath, true);
			} finally {
				SetupEvent.Set();
			}
		}

		[Test]
		public void StartSingleManager() {
			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsEmpty(networkProfile.ManagerServers);
			Assert.IsFalse(machine.IsManager);
			networkProfile.StartService(LocalAddresses[0], ServiceType.Manager);
			networkProfile.RegisterManager(LocalAddresses[0]);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsNotEmpty(networkProfile.ManagerServers);
			Assert.IsTrue(machine.IsManager);
		}

		[Test]
		public void StartMultipleManagers() {
			for (int i = 0; i < LocalAddresses.Length; i++) {
				MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[i]);
				Assert.IsNotNull(machine);
				Assert.IsEmpty(networkProfile.ManagerServers);
				Assert.IsFalse(machine.IsManager);
				networkProfile.StartService(LocalAddresses[i], ServiceType.Manager);
				networkProfile.RegisterManager(LocalAddresses[i]);

				networkProfile.Refresh();

				machine = networkProfile.GetMachineProfile(LocalAddresses[i]);
				Assert.IsNotNull(machine);
				Assert.IsNotEmpty(networkProfile.ManagerServers);
				Assert.IsTrue(machine.IsManager);
			}
		}

		[Test]
		public void StartSingleRoot() {
			StartSingleManager();

			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			networkProfile.StartService(LocalAddresses[0], ServiceType.Root);
			networkProfile.RegisterRoot(LocalAddresses[0]);
		}

		[Test]
		public void StartMultipleRoots() {
			StartSingleManager();

			for (int i = 0; i < LocalAddresses.Length; i++) {
				MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[i]);
				Assert.IsNotNull(machine);
				Assert.IsFalse(machine.IsRoot);
				networkProfile.StartService(LocalAddresses[i], ServiceType.Root);
				networkProfile.RegisterRoot(LocalAddresses[i]);
			}
		}

		[Test]
		public void StartSingleBlock() {
			StartSingleManager();

			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
			networkProfile.StartService(LocalAddresses[0], ServiceType.Block);
			networkProfile.RegisterBlock(LocalAddresses[0]);
		}

		[Test]
		public void StartMultipleBlocks() {
			StartSingleManager();

			for (int i = 0; i < LocalAddresses.Length; i++) {
				MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[i]);
				Assert.IsNotNull(machine);
				Assert.IsFalse(machine.IsBlock);
				networkProfile.StartService(LocalAddresses[i], ServiceType.Block);
				networkProfile.RegisterBlock(LocalAddresses[i]);
			}
		}

		[Test]
		public void StartAllServices() {
			StartSingleManager();

			networkProfile.Refresh();

			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			networkProfile.StartService(LocalAddresses[0], ServiceType.Root);
			networkProfile.RegisterRoot(LocalAddresses[0]);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
			networkProfile.StartService(LocalAddresses[0], ServiceType.Block);
			networkProfile.RegisterBlock(LocalAddresses[0]);
		}

		[Test]
		public void StartAndStopManager() {
			StartSingleManager();

			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsManager);

			networkProfile.StopService(LocalAddresses[0], ServiceType.Manager);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsManager);
		}

		[Test]
		public void StartAndStopRoot() {
			StartSingleRoot();

			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsRoot);

			networkProfile.StopService(LocalAddresses[0], ServiceType.Root);
			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
		}

		[Test]
		public void StartAndStopBlock() {
			StartSingleBlock();

			MachineProfile machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsBlock);

			networkProfile.StopService(LocalAddresses[0], ServiceType.Block);

			networkProfile.Refresh();

			machine = networkProfile.GetMachineProfile(LocalAddresses[0]);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
		}

		[Test]
		public void StartAndStopAllServices() {
			StartAllServices();

			//TODO:
		}
	}
}