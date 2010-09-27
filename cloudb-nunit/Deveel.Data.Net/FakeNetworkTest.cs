﻿using System;
using System.IO;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory)]
	[TestFixture(NetworkStoreType.FileSystem)]
	public class FakeNetworkTest {
		private NetworkProfile networkProfile;
		private FakeAdminService adminService;
		private NetworkStoreType storeType;
		private string path;
		
		public FakeNetworkTest(NetworkStoreType storeType) {
			this.storeType = storeType;
		}
		
		protected void Config(ConfigSource config) {
			if (storeType == NetworkStoreType.FileSystem) {
				path = Path.Combine(Environment.CurrentDirectory, "base");
				if (Directory.Exists(path))
					Directory.Delete(path, true);
			
				Directory.CreateDirectory(path);
			
				config.SetValue("node_directory", path);
			}
		}
		
		protected void OnSetUp() {
		}
		
		protected void OnTearDown() {
			if (storeType == NetworkStoreType.FileSystem &&
			    Directory.Exists(path))
				Directory.Delete(path, true);
		}
		
		[SetUp]
		public void SetUp() {
			adminService = new FakeAdminService(storeType);
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