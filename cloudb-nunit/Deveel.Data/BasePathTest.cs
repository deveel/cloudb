using System;

using Deveel.Data.Net;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class BasePathTest {
		private NetworkProfile networkProfile;
		private FakeAdminService adminService;

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";

		[SetUp]
		public void SetUp() {
			adminService = new FakeAdminService();
			networkProfile = new NetworkProfile(new FakeServiceConnector(adminService));
			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(FakeServiceAddress.Local);
			networkProfile.Configuration = netConfig;


			// start a fake network to test in-memory ...
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Block);
			networkProfile.RegisterBlock(FakeServiceAddress.Local);
			networkProfile.Refresh();
		}

		[Test]
		public void TestAddPath() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));
		}

		[Test]
		public void TestConnectAndDisconnectClient() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();
			Assert.IsTrue(client.IsConnected);

			client.Dispose();
			Assert.IsFalse(client.IsConnected);
		}

		[Test]
		public void TestCreateTransaction() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);
			DbTransaction transaction = session.CreateTransaction();
			Assert.IsNotNull(transaction);
			transaction.Dispose();
		}

		[Test]
		public void TestCreateTable() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using(DbTransaction transaction = session.CreateTransaction()) {
				try {
					if (transaction.CreateTable("comics")) {
						DbTable table = transaction.GetTable("comics");
						table.Schema.AddColumn("name");
						table.Schema.AddColumn("editor");
						table.Schema.AddColumn("issue");
						table.Schema.AddColumn("year");
						table.Schema.AddIndex("year");
						table.Schema.AddIndex("editor");

						transaction.Commit();
					}
				} catch(Exception e) {
					Assert.Fail(e.Message);
				}
			}

			using(DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
					Assert.IsNotNull(table);
					Assert.AreEqual("comics", table.Name);
					Assert.AreEqual(4, table.Schema.ColumnCount);
					Assert.AreEqual(2, table.Schema.IndexedColumns.Length);
				} catch(Exception e) {
					Assert.Fail(e.Message);
				}
			}
		}

		[Test]
		public void TestCreateTableAndInsertData() {
			
		}

		private static bool PathProfileExists(PathProfile profile) {
			if (profile.Path != PathName)
				return false;
			if (profile.PathType != PathTypeName)
				return false;
			return true;
		}
	}
}