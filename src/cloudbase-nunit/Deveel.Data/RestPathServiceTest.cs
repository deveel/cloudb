using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Xml;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

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
		private RestPathClientService pathService;
		private string path;

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";
		private const string NetworkPassword = "123456";

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);

		private static readonly TcpServiceAddress Local = new TcpServiceAddress("127.0.0.1", 1587);
		private static readonly HttpServiceAddress LocalPath = new HttpServiceAddress("localhost", 1588, PathName);
		
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

			config.SetValue("logger." + Logger.NetworkLoggerName + ".type", "simple-console");
			Logger.Init(config);
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), Logger.Network.BaseLogger);
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

		private Deveel.Data.Net.Client.IMessageSerializer GetMethodSerializer() {
			if (format == HttpMessageFormat.Json)
				throw new NotSupportedException();
			if (format == HttpMessageFormat.Xml)
				return new XmlRestMessageSerializer();
			throw new NotSupportedException();
		}
		
		private static TableResponse ReadXmlResponse(StreamReader reader) {
			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.Load(reader);
			
			string resourceName = xmlDoc.DocumentElement.Attributes["name"].Value;
			long rowCount = Int64.Parse(xmlDoc.DocumentElement.Attributes["rows"].Value);
			long columnCount = Int64.Parse(xmlDoc.DocumentElement.Attributes["columns"].Value);

			TableResponse response = new TableResponse(resourceName, columnCount, rowCount);

			foreach(XmlElement child in xmlDoc.DocumentElement.ChildNodes) {
				if (child.LocalName == "column") {
					string name = child.Attributes["name"].Value;
					bool indexed = Boolean.Parse(child.Attributes["indexed"].Value);
					response.Columns[name] = new TableColumn(name, indexed);
				} else if (child.LocalName == "row") {
					int rowid = Int32.Parse(child.Attributes["id"].Value);
					string href = child.Attributes["href"].Value;
					response.Rows.Add(new TableRow(rowid, href));
				}
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

			IServiceFactory delegator = null;
			if (storeType == NetworkStoreType.Memory) {
				delegator = new MemoryServiceFactory();
			} else if (storeType == NetworkStoreType.FileSystem) {
				delegator = new FileSystemServiceFactory(path);
			}

			adminService = new TcpAdminService(delegator, Local, NetworkPassword);
			adminService.Config = netConfig;
			adminService.Start();

			networkProfile = new NetworkProfile(new TcpServiceConnector(NetworkPassword));
			networkProfile.Configuration = netConfig;
			
			// start a network to test in-memory ...
			networkProfile.StartService(Local, ServiceType.Manager);
			networkProfile.StartService(Local, ServiceType.Root);
			networkProfile.RegisterRoot(Local);
			networkProfile.StartService(Local, ServiceType.Block);
			networkProfile.RegisterBlock(Local);

			// Add the path ...
			networkProfile.AddPath(PathName, PathTypeName, Local, new IServiceAddress[] {Local});
			networkProfile.Refresh();

			SetUpPath();

			pathService = new RestPathClientService(LocalPath, Local, new TcpServiceConnector(NetworkPassword));
			pathService.MessageSerializer = GetMethodSerializer();
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
			sb.Append("comics");
			sb.Append("/");
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
			Assert.AreEqual(4, table.Columns.Count);
		}
		
		[Test]
		public void GetOne() {
			StringBuilder sb = new StringBuilder(LocalPath.ToUri().ToString());
			sb.Append("comics");
			sb.Append("/");
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sb.ToString());
			request.Method = "GET";

			HttpWebResponse response = (HttpWebResponse)request.GetResponse();
			Assert.IsTrue(response.StatusCode == HttpStatusCode.OK);

			TableResponse table;
			using (StreamReader reader = new StreamReader(response.GetResponseStream())) {
				if (format == HttpMessageFormat.Xml) {
					table = ReadXmlResponse(reader);
				} else {
					table = ReadJsonResponse(reader);
				}
			}

			request = (HttpWebRequest)WebRequest.Create(table.Rows[0].Href);
			request.Method = "GET";

			response = (HttpWebResponse)request.GetResponse();
			Assert.IsTrue(response.StatusCode == HttpStatusCode.OK);

			TableRow row;

			using(StreamReader reader = new StreamReader(response.GetResponseStream())) {
				if (format == HttpMessageFormat.Json) {
					row = ReadJsonRow(reader);
				} else {
					row = ReadXmlRow(reader);
				}
			}

			Assert.AreNotEqual(-1, row.Id);
			Assert.AreEqual(4, row.Values.Count);

			foreach(KeyValuePair<string, string> pair in row.Values) {
				//TODO:
			}
		}

		[Test]
		public void DeleteOne() {
			StringBuilder sb = new StringBuilder(LocalPath.ToUri().ToString());
			sb.Append("comics");
			sb.Append("/");
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sb.ToString());
			request.Method = "GET";

			HttpWebResponse response = (HttpWebResponse)request.GetResponse();
			Assert.IsTrue(response.StatusCode == HttpStatusCode.OK);

			TableResponse table;
			using (StreamReader reader = new StreamReader(response.GetResponseStream())) {
				if (format == HttpMessageFormat.Xml) {
					table = ReadXmlResponse(reader);
				} else {
					table = ReadJsonResponse(reader);
				}
			}

			request = (HttpWebRequest)WebRequest.Create(table.Rows[0].Href);
			request.Method = "DELETE";

			response = (HttpWebResponse)request.GetResponse();
			Assert.AreEqual(204, (int) response.StatusCode);
		}

		[Test]
		public void UpdateOne() {
			StringBuilder sb = new StringBuilder(LocalPath.ToUri().ToString());
			sb.Append("comics");
			sb.Append("/");
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sb.ToString());
			request.Method = "GET";

			HttpWebResponse response = (HttpWebResponse)request.GetResponse();
			Assert.IsTrue(response.StatusCode == HttpStatusCode.OK);

			TableResponse table;
			using (StreamReader reader = new StreamReader(response.GetResponseStream())) {
				if (format == HttpMessageFormat.Xml) {
					table = ReadXmlResponse(reader);
				} else {
					table = ReadJsonResponse(reader);
				}
			}

			request = (HttpWebRequest)WebRequest.Create(table.Rows[0].Href);
			request.Method = "GET";

			response = (HttpWebResponse)request.GetResponse();
			Assert.IsTrue(response.StatusCode == HttpStatusCode.OK);

			TableRow row;

			using (StreamReader reader = new StreamReader(response.GetResponseStream())) {
				if (format == HttpMessageFormat.Json) {
					row = ReadJsonRow(reader);
				} else {
					row = ReadXmlRow(reader);
				}
			}

			row.Values["year"] = "2001";
			request = (HttpWebRequest)WebRequest.Create(table.Rows[0].Href);
			request.Method = "PUT";
			request.ContentType = (format == HttpMessageFormat.Xml ? "text/xml" : "application/json");

			Stream output = request.GetRequestStream();
			WriteRowToStream(output, row);
			output.Flush();
			output.Close();

			response = (HttpWebResponse)request.GetResponse();
			Assert.AreEqual(201, (int)response.StatusCode);
		}

		private void WriteRowToStream(Stream output, TableRow row) {
			StreamWriter writer = new StreamWriter(output);
			if (format == HttpMessageFormat.Xml)
				WriteRowToXml(writer, row);
			else
				WriteRowToJson(writer, row);

			writer.Flush();
		}

		private static void WriteRowToXml(TextWriter writer, TableRow row) {
			XmlTextWriter xmlWriter = new XmlTextWriter(writer);
			xmlWriter.WriteStartDocument(true);
			xmlWriter.WriteStartElement("row");
			xmlWriter.WriteStartAttribute("id");
			xmlWriter.WriteValue(row.Id);
			xmlWriter.WriteEndAttribute();
			foreach(KeyValuePair<string, string> pair in row.Values) {
				xmlWriter.WriteStartElement(pair.Key);
				xmlWriter.WriteValue(pair.Value);
				xmlWriter.WriteEndElement();
			}
			xmlWriter.WriteEndElement();
			xmlWriter.WriteEndDocument();
		}

		private static void WriteRowToJson(TextWriter writer, TableRow row) {
			throw new NotImplementedException();
		}

		private static TableRow ReadJsonRow(TextReader reader) {
			throw new NotImplementedException();
		}

		private static TableRow ReadXmlRow(TextReader reader) {
			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.Load(reader);

			long rowid = Int64.Parse(xmlDoc.DocumentElement.Attributes["id"].Value);
			TableRow row = new TableRow(rowid, null);

			foreach(XmlNode child in xmlDoc.DocumentElement.ChildNodes) {
				row.Values.Add(child.LocalName, child.InnerText);
			}

			return row;
		}

		#region TableColumn

		public class TableColumn {
			public readonly string Name;
			public readonly bool Indexed;

			public TableColumn(string name, bool indexed) {
				Name = name;
				Indexed = indexed;
			}
		}

		#endregion

		#region TableRow

		public class TableRow {
			public readonly long Id;
			public readonly string Href;
			public readonly Dictionary<string, string> Values;
			
			public TableRow(long id, string href) {
				Id = id;
				Href = href;
				Values = new Dictionary<string, string>();
			}
		}
		
		#endregion
		
		#region TableResponse
		
		public class TableResponse {
			public readonly long RowCount;
			public readonly long ColumnCount;
			public readonly string ResourceName;
			public readonly List<TableRow> Rows;
			public readonly Dictionary<string, TableColumn> Columns;
			
			public TableResponse(string resourceName, long columns, long rows) {
				ResourceName = resourceName;
				Rows = new List<TableRow>();
				Columns = new Dictionary<string, TableColumn>((int)columns);
				ColumnCount = columns;
				RowCount = rows;
			}
		}
		
		#endregion
	}
}