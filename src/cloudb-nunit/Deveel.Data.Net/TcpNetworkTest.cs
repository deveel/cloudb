using System;
using System.Net;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(StoreType.FileSystem)]
	[TestFixture(StoreType.Memory)]
	public sealed class TcpNetworkTest : NetworkServiceTestBase {
		private const string NetworkPassword = "123456";
		
		private static readonly TcpServiceAddress Local = new TcpServiceAddress(IPAddress.Loopback);

		
		public TcpNetworkTest(StoreType storeType)
			: base(storeType) {
		}

		protected override IServiceAddress LocalAddress {
			get { return Local; }
		}

		protected override AdminService CreateAdminService(StoreType storeType) {
			IServiceFactory serviceFactory = null;
			if (storeType == StoreType.Memory) {
				serviceFactory = new MemoryServiceFactory();
			} else if (storeType == StoreType.FileSystem) {
				serviceFactory = new FileSystemServiceFactory(TestPath);
			}

			return new TcpAdminService(serviceFactory, Local, NetworkPassword);
		}

		protected override IServiceConnector CreateConnector() {
			return new TcpServiceConnector(NetworkPassword);
		}
	}
}