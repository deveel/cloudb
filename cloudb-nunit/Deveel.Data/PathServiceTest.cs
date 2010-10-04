using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory, HttpMessageFormat.Xml)]
	[TestFixture(NetworkStoreType.Memory, HttpMessageFormat.Json)]
	public sealed class PathServiceTest {
		private readonly HttpMessageFormat format;
		private readonly NetworkStoreType storeType;
		
		private NetworkProfile networkProfile;
		private HttpAdminService adminService;
		private readonly NetworkStoreType storeType;
		
		public PathServiceTest(NetworkStoreType storeType, HttpMessageFormat format) {
			this.format = format;
			this.storeType = storeType;
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
			adminService.Init();

			networkProfile = new NetworkProfile(new HttpServiceConnector("foo", "foo"));
			networkProfile.Configuration = netConfig;
			
			// start a network to test in-memory ...
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Block);
			networkProfile.RegisterBlock(FakeServiceAddress.Local);
			networkProfile.Refresh();

			SetupEvent.Set();

		}
	}
}