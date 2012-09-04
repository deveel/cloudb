using System;
using System.Reflection;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public abstract class PathTestBase : NetworkTestBase {
		private NetworkClient client;

		protected PathTestBase(StoreType storeType) 
			: base(storeType) {
		}

		protected override IServiceAddress LocalAddress {
			get { return FakeServiceAddress.Local; }
		}

		protected override AdminService CreateAdminService(StoreType storeType) {
			return new FakeAdminService(storeType);
		}

		protected override IServiceConnector CreateConnector() {
			return new FakeServiceConnector((FakeAdminService)AdminService);
		}

		protected virtual string PathName {
			get { return "testdb"; }
		}

		protected abstract string PathType { get; }

		protected NetworkClient Client {
			get { return client; }
		}

		protected override void OnSetUp() {
			SetupAppDomain();
			NetworkProfile.StartService(LocalAddress, ServiceType.Manager);
			NetworkProfile.RegisterManager(LocalAddress);
			NetworkProfile.StartService(LocalAddress, ServiceType.Root);
			NetworkProfile.RegisterRoot(LocalAddress);
			NetworkProfile.StartService(LocalAddress, ServiceType.Block);
			NetworkProfile.RegisterBlock(LocalAddress);

			NetworkProfile.AddPathToNetwork(PathName, PathType, LocalAddress, new IServiceAddress[] {LocalAddress});

			client = new NetworkClient(LocalAddress, new FakeServiceConnector((FakeAdminService)AdminService));
			client.ConnectNetwork();
		}

		private void SetupAppDomain() {
		}

		protected override void OnTearDown() {
			client.Disconnect();

			NetworkProfile.StopService(LocalAddress, ServiceType.Block);
			NetworkProfile.DeregisterBlock(LocalAddress);
			NetworkProfile.StopService(LocalAddress, ServiceType.Root);
			NetworkProfile.DeregisterRoot(LocalAddress);
			NetworkProfile.StopService(LocalAddress, ServiceType.Manager);
			NetworkProfile.DeregisterManager(LocalAddress);
		}
	}
}