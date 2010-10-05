using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Xml;

using Deveel.Data.Diagnostics;
using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture(NetworkStoreType.Memory, HttpMessageFormat.Xml)]
	[TestFixture(NetworkStoreType.Memory, HttpMessageFormat.Json)]
	[TestFixture(NetworkStoreType.FileSystem, HttpMessageFormat.Xml)]
	[TestFixture(NetworkStoreType.FileSystem, HttpMessageFormat.Json)]
	public sealed class RestPathServiceTest {
		private readonly HttpMessageFormat format;
		private readonly NetworkStoreType storeType;
		
		private NetworkProfile networkProfile;
		private TcpAdminService adminService;
		private RestPathService pathService;
		private string path;

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";
		private const string NetworkPassword = "123456";

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);

		private static readonly TcpServiceAddress Local = new TcpServiceAddress("127.0.0.1", 1587);
		private static readonly HttpServiceAddress LocalPath = new HttpServiceAddress("localhost", 1588);
		
		public RestPathServiceTest(NetworkStoreType storeType, HttpMessageFormat format) {
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
			NetworkClient client = new NetworkClient(Local, new TcpServiceConnector(NetworkPassword));
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
		
		private static TableResponse ReadXmlResponse(StreamReader reader) {
			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.Load(reader);
			
			string resourceName = xmlDoc.DocumentElement.LocalName;
			TableResponse response = new TableResponse(resourceName);
			
			foreach(XmlElement rowElem in xmlDoc.DocumentElement.ChildNodes) {
				int rowid = Int32.Parse(rowElem.Attributes["id"].Value);
				
				TableRow row = new TableRow(rowid);
				
				foreach(XmlElement valueElem in rowElem.ChildNodes) {
					row.Values[valueElem.LocalName] = valueElem.Value;
				}
				
				response.Rows[rowid] = row;
			}
			
			return response;
		}
		
		private static TableResponse ReadJsonResponse(StreamReader reader) {
			throw new NotImplementedException();
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

			adminService = new TcpAdminService(delegator, Local, NetworkPassword);
			adminService.Config = netConfig;
			adminService.Init();

			networkProfile = new NetworkProfile(new TcpServiceConnector(NetworkPassword));
			networkProfile.Configuration = netConfig;
			
			// start a network to test in-memory ...
			networkProfile.StartService(Local, ServiceType.Manager);
			networkProfile.StartService(Local, ServiceType.Root);
			networkProfile.RegisterRoot(Local);
			networkProfile.StartService(Local, ServiceType.Block);
			networkProfile.RegisterBlock(Local);

			// Add the path ...
			networkProfile.AddPath(Local, PathName, PathTypeName);
			networkProfile.Refresh();

			SetUpPath();

			pathService = new RestPathService(LocalPath, Local, new TcpServiceConnector(NetworkPassword));
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
		public void GetAll() {
			StringBuilder sb = new StringBuilder(LocalPath.ToUri().ToString());
			sb.Append(PathName);
			sb.Append("/");
			sb.Append("comics");
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sb.ToString());
			request.Method = "GET";
			
			HttpWebResponse response = (HttpWebResponse) request.GetResponse();
			Assert.IsTrue(response.StatusCode == HttpStatusCode.OK);
			
			TableResponse table;
			using (StreamReader reader = new StreamReader(response.GetResponseStream())) {
				if (format == HttpMessageFormat.Xml) {
					table = ReadXmlResponse(reader);
				} else {
					table = ReadJsonResponse(reader);
				}
			}
			
			Assert.AreEqual("comics", table.ResourceName);
			Assert.AreEqual(5, table.Rows.Count);
			
			foreach(TableRow row in table.Rows.Values) {
				Assert.AreEqual(4, row.Values.Count);
				Assert.IsTrue(row.Values.ContainsKey("name"));
				Assert.IsTrue(row.Values.ContainsKey("editor"));
				Assert.IsTrue(row.Values.ContainsKey("issue"));
				Assert.IsTrue(row.Values.ContainsKey("year"));
			}
		}
		
		[Test]
		public void GetOne() {
			
		}
		
		#region TableRow
		
		public class TableRow {
			private readonly int id;
			public readonly Dictionary<string, string> Values;
			
			public TableRow(int id) {
				this.id = id;
				Values = new Dictionary<string, string>();
			}
		}
		
		#endregion
		
		#region Table Response
		
		public class TableResponse {
			public readonly string ResourceName;
			public readonly Dictionary<int, TableRow> Rows;
			
			public TableResponse(string resourceName) {
				ResourceName = resourceName;
				Rows = new Dictionary<int, RestPathServiceTest.TableRow>();
			}
		}
		
		#endregion
	}
}