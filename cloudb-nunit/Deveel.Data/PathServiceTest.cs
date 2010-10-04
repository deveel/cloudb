using System;
using System.IO;
using System.Threading;

using Deveel.Data.Diagnostics;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory, HttpMessageFormat.Xml)]
	[TestFixture(NetworkStoreType.Memory, HttpMessageFormat.Json)]
	public sealed class PathServiceTest {
		private readonly HttpMessageFormat format;
		private readonly NetworkStoreType storeType;
		
		private NetworkProfile networkProfile;
		private HttpAdminService adminService;
		private HttpPathService pathService;
		private string path;

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);

		private static readonly HttpServiceAddress Local = new HttpServiceAddress("localhost", 1587);
		private static readonly HttpServiceAddress LocalPath = new HttpServiceAddress("localhost", 1588);
		
		public PathServiceTest(NetworkStoreType storeType, HttpMessageFormat format) {
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

		private static void SetUpPath() {
			NetworkClient client = new NetworkClient(Local, new HttpServiceConnector());
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table;
					if (transaction.CreateTable("comics")) {
						table = transaction.GetTable("comics");
						table.Schema.AddColumn("name");
						table.Schema.AddColumn("editor");
						table.Schema.AddColumn("issue");
						table.Schema.AddColumn("year");
						table.Schema.AddIndex("year");
						table.Schema.AddIndex("editor");
					} else {
						table = transaction.GetTable("comics");
					}

					DbRow row = table.NewRow();
					row.SetValue("name", "Fantastic Four");
					row.SetValue("issue", "1");
					row.SetValue("year", "1961");
					row.SetValue("editor", "Marvel");
					table.Insert(row);

					row = table.NewRow();
					row.SetValue("name", "Detective Comics");
					row.SetValue("issue", "27");
					row.SetValue("year", "1939");
					row.SetValue("editor", "DC Comics");
					table.Insert(row);

					row = table.NewRow();
					row.SetValue("name", "Amazing Fantasy");
					row.SetValue("issue", "15");
					row.SetValue("year", "1962");
					row.SetValue("editor", "Marvel");
					table.Insert(row);

					row = table.NewRow();
					row.SetValue("name", "The Amazing Spiderman");
					row.SetValue("issue", "1");
					row.SetValue("year", "1963");
					row.SetValue("editor", "Marvel");
					table.Insert(row);

					row = table.NewRow();
					row.SetValue("name", "All-American Comics");
					row.SetValue("issue", "16");
					row.SetValue("year", "1940");
					row.SetValue("editor", "DC Comice");
					table.Insert(row);

					Assert.IsTrue(table.IsModified);

					transaction.Commit();
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}
		}

		private IMethodSerializer GetMethodSerializer() {
			if (format == HttpMessageFormat.Json)
				throw new NotSupportedException();
			if (format == HttpMessageFormat.Xml)
				return new XmlMethodSerializer();
			throw new NotSupportedException();
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

			// Add the path ...
			networkProfile.AddPath(Local, PathName, PathTypeName);
			networkProfile.Refresh();

			SetUpPath();

			pathService = new HttpPathService(LocalPath, Local);
			pathService.MethodSerializer = GetMethodSerializer();
			pathService.Init();

			SetupEvent.Set();
		}

		[TearDown]
		public void TearDown() {
			adminService.Dispose();
			pathService.Dispose();

			if (storeType == NetworkStoreType.FileSystem &&
				Directory.Exists(path))
				Directory.Delete(path, true);

			SetupEvent.Set();
		}

		[Test]
		public void TestGet() {
		}
	}
}