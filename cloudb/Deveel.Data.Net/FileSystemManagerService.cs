using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class FileSystemManagerService : ManagerService {
		private readonly string basePath;
		private readonly string dbPath;
		private FileSystemDatabase database;
		
		private const string RegisteredBlockServers = "blockservers";
		private const string RegisteredRootServers = "rootservers";

		public FileSystemManagerService(IServiceConnector connector, string basePath, 
		                               string dbPath, ServiceAddress address)
			: base(connector, address) {
			this.basePath = basePath;
			this.dbPath = dbPath;
		}
		
		protected override void OnInit() {
			database = new FileSystemDatabase(dbPath);
			database.Start();

			SetBlockDatabase(database);

			// Read all the registered block servers that were last persisted and
			// populate the manager with them,
			string f = Path.Combine(basePath, RegisteredBlockServers);
			if (File.Exists(f)) {
				using (StreamReader rin = new StreamReader(f)) {
					string line;
					while ((line = rin.ReadLine()) != null) {
						int p = line.IndexOf(",");
						long guid = Int64.Parse(line.Substring(0, p));
						ServiceAddress addr = ServiceAddress.Parse(line.Substring(p + 1));
						AddRegisteredBlockServer(guid, addr);
					}
				}
			}

			// Read all the registered root servers that were last persisted and
			// populate the manager with them,
			f = Path.Combine(basePath, RegisteredRootServers);
			if (File.Exists(f)) {
				using (StreamReader rin = new StreamReader(f)) {
					string line;
					while ((line = rin.ReadLine()) != null) {
						ServiceAddress addr = ServiceAddress.Parse(line);
						AddRegisteredRootServer(addr);
					}
				}
			}
		}
		
		protected override void PersistBlockServers(IList<BlockServerInfo> servers_list) {
			try {
				string f = Path.Combine(basePath, RegisteredBlockServers);
				if (File.Exists(f))
					File.Delete(f);

				using(FileStream fileStream = File.Create(f)) {
					using (StreamWriter output = new StreamWriter(fileStream)) {
						foreach (BlockServerInfo s in servers_list) {
							output.Write(s.Guid);
							output.Write(",");
							output.WriteLine(s.Address.ToString());
						}

						output.Flush();
					}
				}
			} catch (IOException e) {
				throw new ApplicationException("Error persisting block service list: " +
												 e.Message);
			}
		}
		
		protected override void PersistRootServers(IList<RootServerInfo> servers_list) {
			try {
				string f = Path.Combine(basePath, RegisteredRootServers);
				if (File.Exists(f))
					File.Delete(f);

				using (FileStream fileStream = File.Create(f)) {
					using (StreamWriter output = new StreamWriter(fileStream)) {
						foreach (RootServerInfo s in servers_list)
							output.WriteLine(s.Address.ToString());
						output.Flush();
					}
				}
			} catch (IOException e) {
				throw new ApplicationException("Error persisting root service list: " +
										   e.Message);
			}
		}
		
		protected override void OnDispose(bool disposing) {
			base.OnDispose(disposing);
			
			if (disposing) {
				database.Stop();
				database = null;
			}
		}
	}
}