using System;
using System.Net;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.FileSystem)]
	[TestFixture(NetworkStoreType.Memory)]
	public sealed class TcpNetworkTest : NetworkTestBase {
		private const string NetworkPassword = "123456";
		
		private static readonly TcpServiceAddress Local = new TcpServiceAddress(IPAddress.Loopback);

		
		public TcpNetworkTest(NetworkStoreType storeType)
			: base(storeType) {
		}

		protected override IServiceAddress[] LocalAddresses {
			get { return new IServiceAddress[] {Local}; }
		}

		protected override AdminService CreateAdminService(NetworkStoreType storeType) {
			IServiceFactory serviceFactory = null;
			if (storeType == NetworkStoreType.Memory) {
				serviceFactory = new MemoryServiceFactory();
			} else if (storeType == NetworkStoreType.FileSystem) {
				serviceFactory = new FileSystemServiceFactory(TestPath);
			}

			return new TcpAdminService(serviceFactory, Local, NetworkPassword);
		}

		protected override IServiceConnector CreateConnector() {
			return new TcpServiceConnector(NetworkPassword);
		}
	}
}