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
		private const string RegisteredManagerServers = "managerservers";
		private const String ManagerProperties = "manager.properties";

		public FileSystemManagerService(IServiceConnector connector, string basePath, 
		                               string dbPath, IServiceAddress address)
			: base(connector, address) {
			this.basePath = basePath;
			this.dbPath = dbPath;
		}
		
		protected override void OnStart() {
			database = new FileSystemDatabase(dbPath);
			database.Start();

			SetBlockDatabase(database);

			// Read the unique id value,
			string f = Path.Combine(basePath, ManagerProperties);
			if (File.Exists(f)) {
				TextReader rin = new StreamReader(f);
				while (true) {
					string line = rin.ReadLine();
					if (line == null)
						break;

					if (line.StartsWith("id="))
						UniqueId = Int32.Parse(line.Substring(3));
				}
				rin.Close();
			}

			// Read all the registered block servers that were last persisted and
			// populate the manager with them,
			f = Path.Combine(basePath, RegisteredBlockServers);
			if (File.Exists(f)) {
				using (StreamReader rin = new StreamReader(f)) {
					string line;
					while ((line = rin.ReadLine()) != null) {
						int p = line.IndexOf(",");
						long guid = Int64.Parse(line.Substring(0, p));
						IServiceAddress addr = ServiceAddresses.ParseString(line.Substring(p + 1));
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
						IServiceAddress addr = ServiceAddresses.ParseString(line);
						AddRegisteredRootServer(addr);
					}
				}
			}

			// Read all the registered manager servers that were last persisted and
			// populate the manager with them,
			f = Path.Combine(basePath, RegisteredManagerServers);
			if (File.Exists(f)) {
				TextReader rin = new StreamReader(f);
				while (true) {
					string line = rin.ReadLine();
					if (line == null) {
						break;
					}
					AddRegisteredManagerServer(ServiceAddresses.ParseString(line));
				}
				rin.Close();
			}

			// Perform the initialization procedure (contacts the other managers and
			// syncs data).
			base.OnStart();
		}

		protected override void OnStop() {
			if (database != null) {
				database.Stop();
				database = null;
			}

			base.OnStop();
		}

		protected override void PersistManagerServers(IList<ManagerServerInfo> servers) {
			Stream fileStream = null;
			try {
				string f = Path.Combine(basePath, RegisteredManagerServers);
				if (File.Exists(f)) 
					File.Delete(f);
					
				fileStream = File.Create(f);

				StreamWriter fr = new StreamWriter(fileStream);
				foreach (ManagerServerInfo s in servers) {
					fr.WriteLine(s.Address.ToString());
				}

				fr.Flush();
				fr.Close();

			} catch (IOException e) {
				throw new ApplicationException("Error persisting manager server list: " + e.Message);
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
		}

		protected override void PersistUniqueId(int unique_id) {
			Stream filestream = null;
			try {
				string f = Path.Combine(basePath, ManagerProperties);
				if (File.Exists(f))
					File.Delete(f);
					
				filestream = File.Create(f);

				StreamWriter fr = new StreamWriter(filestream);
				fr.WriteLine("id=" + unique_id);

				fr.Flush();
			} catch (Exception e) {
				throw new ApplicationException(e.Message, e);
			} finally {
				if (filestream != null)
					filestream.Close();
			}
		}

		protected override void PersistBlockServers(IList<BlockServerInfo> servers) {
			try {
				string f = Path.Combine(basePath, RegisteredBlockServers);
				if (File.Exists(f))
					File.Delete(f);

				using(FileStream fileStream = File.Create(f)) {
					using (StreamWriter output = new StreamWriter(fileStream)) {
						foreach (BlockServerInfo s in servers) {
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
		
		protected override void PersistRootServers(IList<RootServerInfo> servers) {
			try {
				string f = Path.Combine(basePath, RegisteredRootServers);
				if (File.Exists(f))
					File.Delete(f);

				using (FileStream fileStream = File.Create(f)) {
					using (StreamWriter output = new StreamWriter(fileStream)) {
						foreach (RootServerInfo s in servers)
							output.WriteLine(s.Address.ToString());
						output.Flush();
					}
				}
			} catch (IOException e) {
				throw new ApplicationException("Error persisting root service list: " +
										   e.Message);
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				database.Stop();
				database = null;		
			}

			base.Dispose(disposing);
		}		
	}
}