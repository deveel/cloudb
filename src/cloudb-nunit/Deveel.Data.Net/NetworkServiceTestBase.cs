using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public abstract class NetworkServiceTestBase : NetworkTestBase {
		protected NetworkServiceTestBase(NetworkStoreType storeType) 
			: base(storeType) {
		}

		[Test]
		public void StartManager() {
			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsEmpty(NetworkProfile.GetManagerServers());
			Assert.IsFalse(machine.IsManager);
			NetworkProfile.StartService(LocalAddress, ServiceType.Manager);
			NetworkProfile.RegisterManager(LocalAddress);

			NetworkProfile.Refresh();

			machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsNotNull(NetworkProfile.GetManagerServers());
			Assert.IsTrue(machine.IsManager);
		}

		[Test]
		public void StartRoot() {
			StartManager();

			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			NetworkProfile.StartService(LocalAddress, ServiceType.Root);
			NetworkProfile.RegisterRoot(LocalAddress);
		}

		[Test]
		public void StartBlock() {
			StartManager();

			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
			NetworkProfile.StartService(LocalAddress, ServiceType.Block);
			NetworkProfile.RegisterBlock(LocalAddress);
		}

		[Test]
		public void StartAllServices() {
			StartManager();

			NetworkProfile.Refresh();

			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
			NetworkProfile.StartService(LocalAddress, ServiceType.Root);
			NetworkProfile.RegisterRoot(LocalAddress);

			NetworkProfile.Refresh();

			machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsBlock);
			NetworkProfile.StartService(LocalAddress, ServiceType.Block);
			NetworkProfile.RegisterBlock(LocalAddress);
		}

		[Test]
		public void StartAndStopManager() {
			StartManager();

			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsManager);

			NetworkProfile.StopService(LocalAddress, ServiceType.Manager);

			NetworkProfile.Refresh();

			machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsManager);
		}

		[Test]
		public void StartAndStopRoot() {
			StartRoot();

			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsRoot);

			NetworkProfile.StopService(LocalAddress, ServiceType.Root);
			NetworkProfile.Refresh();

			machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsFalse(machine.IsRoot);
		}

		[Test]
		public void StartAndStopBlock() {
			StartBlock();

			MachineProfile machine = NetworkProfile.GetMachineProfile(LocalAddress);
			Assert.IsNotNull(machine);
			Assert.IsTrue(machine.IsBlock);

			NetworkProfile.StopService(LocalAddress, ServiceType.Block);

			NetworkProfile.Refresh();

			machine = NetworkProfile.GetMachineProfile(LocalAddress);
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