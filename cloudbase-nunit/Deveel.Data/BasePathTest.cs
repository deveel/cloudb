using System;
using System.IO;

using Deveel.Data.Net;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(NetworkStoreType.Memory)]
	[TestFixture(NetworkStoreType.FileSystem)]
	public class BasePathTest {
		private NetworkProfile networkProfile;
		private FakeAdminService adminService;
		private readonly NetworkStoreType storeType;

		public BasePathTest(NetworkStoreType storeType) {
			this.storeType = storeType;
		}

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";

		[SetUp]
		public void SetUp() {
			adminService = new FakeAdminService(storeType);
			NetworkConfigSource config = new NetworkConfigSource();

			if (storeType == NetworkStoreType.FileSystem) {
				string path = Path.Combine(Environment.CurrentDirectory, "base");
				if (Directory.Exists(path))
					Directory.Delete(path, true);
				Directory.CreateDirectory(path);

				config.SetValue("node_directory", path);
			}

			adminService.Config = config;
			adminService.Start();

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

		[TearDown]
		public void TearDown() {
			adminService.Dispose();
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
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using (DbTransaction transaction = session.CreateTransaction()) {
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
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
					Assert.IsNotNull(table);
					Assert.AreEqual("comics", table.Name);
					Assert.AreEqual(4, table.Schema.ColumnCount);
					Assert.AreEqual(2, table.Schema.IndexedColumns.Length);
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
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

			using(DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
					Assert.AreEqual(5, table.RowCount);
				} catch(Exception e) {
					Assert.Fail(e.Message);
				}
			}
		}
		
		[Test]
		public void TestCreateTableAndInsertDataInSameTransaction() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					Assert.IsTrue(transaction.CreateTable("comics"));
						
					DbTable table = transaction.GetTable("comics");
					table.Schema.AddColumn("name");
					table.Schema.AddColumn("editor");
					table.Schema.AddColumn("issue");
					table.Schema.AddColumn("year");
					table.Schema.AddIndex("year");
					table.Schema.AddIndex("editor");
					
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

			using(DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
					Assert.AreEqual(5, table.RowCount);
				} catch(Exception e) {
					Assert.Fail(e.Message);
				}
			}
		}
		
		[Test]
		public void TestInsertAndQueryData() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					Assert.IsTrue(transaction.CreateTable("comics"));
						
					DbTable table = transaction.GetTable("comics");
					table.Schema.AddColumn("name");
					table.Schema.AddColumn("editor");
					table.Schema.AddColumn("issue");
					table.Schema.AddColumn("year");
					table.Schema.AddIndex("year");
					table.Schema.AddIndex("editor");
					
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
					row.SetValue("editor", "DC Comics");
					table.Insert(row);

					Assert.IsTrue(table.IsModified);

					transaction.Commit();
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}

			using(DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
					DbIndex index = table.GetIndex("editor");
					Assert.AreEqual(5, index.Count);
					
					DbIndex marvelIndex = index.Sub("Marvel", true, "Marvel", true);
					Assert.AreEqual(3, marvelIndex.Count);
					
					Console.Out.WriteLine("Marvel Comics");
					foreach(DbRow row in marvelIndex) {
						Console.Out.WriteLine("{0} #{1} ({2})", row["name"], row["issue"], row["year"]);
					}
					
					DbIndex dcIndex = index.Sub("DC Comics", true, "DC Comics", true);
					Assert.AreEqual(2, dcIndex.Count);
					
					Console.Out.WriteLine("DC Comics");
					foreach(DbRow row in dcIndex) {
						Console.Out.WriteLine("{0} #{1} ({2})", row["name"], row["issue"], row["year"]);
					}
				} catch(Exception e) {
					Assert.Fail(e.Message);
				}
			}
		}
		
		[Test]
		public void TestInsertAndUpdateData() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					Assert.IsTrue(transaction.CreateTable("comics"));
						
					DbTable table = transaction.GetTable("comics");
					table.Schema.AddColumn("name");
					table.Schema.AddColumn("editor");
					table.Schema.AddColumn("issue");
					table.Schema.AddColumn("year");
					table.Schema.AddIndex("year");
					table.Schema.AddIndex("editor");
					
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
					row.SetValue("editor", "DC");
					table.Insert(row);

					Assert.IsTrue(table.IsModified);

					transaction.Commit();
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}

			using(DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");
					DbIndex index = table.GetIndex("editor");
					Assert.AreEqual(5, index.Count);
					
					DbIndex dcIndex = index.Sub("DC", true, "DC", true);
					Assert.AreEqual(1, dcIndex.Count);
					
					DbRow row = dcIndex.First;
					row.SetValue("editor", "DC Comics");
					table.Update(row);
					
					index = table.GetIndex("editor");
					dcIndex = index.Sub("DC Comics", true, "DC Comics", true);
					Assert.AreEqual(2, dcIndex.Count);
					
					transaction.Commit();
				} catch(Exception e) {
					Assert.Fail(e.Message);
				}
			}
		}

		[Test]
		public void DeleteSingleRow() {
			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			PathProfile[] pathProfiles = networkProfile.GetPathsFromRoot(FakeServiceAddress.Local);
			Assert.IsTrue(Array.Exists(pathProfiles, PathProfileExists));

			NetworkClient client = new NetworkClient(FakeServiceAddress.Local, new FakeServiceConnector(adminService));
			client.Connect();

			DbSession session = new DbSession(client, PathName);

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					Assert.IsTrue(transaction.CreateTable("comics"));

					DbTable table = transaction.GetTable("comics");
					table.Schema.AddColumn("name");
					table.Schema.AddColumn("editor");
					table.Schema.AddColumn("issue");
					table.Schema.AddColumn("year");
					table.Schema.AddIndex("year");
					table.Schema.AddIndex("editor");

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
					row.SetValue("editor", "DC");
					table.Insert(row);

					Assert.IsTrue(table.IsModified);

					transaction.Commit();
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}

			int deleteCount = 0;

			using (DbTransaction transaction = session.CreateTransaction()) {
				try {
					DbTable table = transaction.GetTable("comics");

					foreach(DbRow row in table) {
						table.Delete(row);
						deleteCount++;
						break;
					}

					transaction.Commit();
				} catch (Exception e) {
					Assert.Fail(e.Message);
				}
			}

			Assert.AreEqual(1, deleteCount);
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