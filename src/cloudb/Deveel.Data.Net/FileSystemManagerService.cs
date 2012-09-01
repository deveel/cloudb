using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class FileSystemManagerService : ManagerService {
		private readonly string basePath;
		private readonly string dbpath;

		private FileSystemDatabase localDb;

		private const string RegisteredBlockServers = "blockservers";
		private const string RegisteredRootServers = "rootservers";
		private const string RegisteredManagerServers = "managerservers";
		private const string ManagerProperties = "manager.properties";


		public FileSystemManagerService(IServiceConnector connector, string basePath, string dbpath, IServiceAddress address)
			: base(connector, address) {
			this.basePath = basePath;
			this.dbpath = dbpath;
		}

		protected override void PersistBlockServers(IList<BlockServiceInfo> serviceList) {
			try {
				string f = Path.Combine(basePath, RegisteredBlockServers);
				if (File.Exists(f))
					File.Delete(f);

				FileStream stream = File.Create(f);

				StreamWriter output = new StreamWriter(stream);
				foreach (BlockServiceInfo s in serviceList) {
					output.Write(s.ServerGuid);
					output.Write(",");
					output.WriteLine(s.Address.ToString());
				}

				output.Flush();
				output.Close();
			} catch (IOException e) {
				throw new ApplicationException("Error persisting block server list: " + e.Message);
			}
		}

		protected override void PersistRootServers(IList<RootServiceInfo> serviceList) {
			try {
				string f = Path.Combine(basePath, RegisteredRootServers);
				if (File.Exists(f)) {
					File.Delete(f);
				}

				FileStream stream = new FileStream(f, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024,
				                                   FileOptions.WriteThrough);
				StreamWriter writer = new StreamWriter(stream);
				foreach (RootServiceInfo s in serviceList) {
					writer.WriteLine(s.Address.ToString());
				}

				writer.Flush();
				writer.Close();

			} catch (IOException e) {
				throw new ApplicationException("Error persisting root server list: " + e.Message);
			}
		}

		protected override void PersistManagerServers(IList<ManagerServiceInfo> serversList) {
			try {
				string f = Path.Combine(basePath, RegisteredManagerServers);
				if (File.Exists(f)) {
					File.Delete(f);
				}

				FileStream stream = new FileStream(f, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024,
												   FileOptions.WriteThrough);
				StreamWriter writer = new StreamWriter(stream);
				foreach (ManagerServiceInfo s in serversList) {
					writer.WriteLine(s.Address.ToString());
				}

				writer.Flush();
				writer.Close();

			} catch (IOException e) {
				throw new ApplicationException("Error persisting root server list: " + e.Message);
			}
		}

		protected override void PersistManagerUniqueId(int uniqueId) {
			try {
				string f = Path.Combine(basePath, ManagerProperties);
				if (File.Exists(f)) {
					File.Delete(f);
				}

				FileStream stream = new FileStream(f, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024,
												   FileOptions.WriteThrough);
				StreamWriter writer = new StreamWriter(stream);
				writer.WriteLine(String.Format("id={0}", uniqueId));
				writer.Flush();
				writer.Close();

			} catch (IOException e) {
				throw new ApplicationException("Error persisting root server list: " + e.Message);
			}
		}

		protected override void OnStart() {
			localDb = new FileSystemDatabase(dbpath);
			localDb.Start();

			SetBlockDatabase(localDb);

			// Read the unique id value,
			string f = Path.Combine(basePath, ManagerProperties);
			if (File.Exists(f)) {
				StreamReader reader = new StreamReader(f);
				string line;
				while ((line = reader.ReadLine()) != null) {
					if (line.StartsWith("id=")) {
						int uniqueId = Int32.Parse(line.Substring(3));
						UniqueManagerId = uniqueId;
					}
				}
				reader.Close();
			}

			// Read all the registered block servers that were last persisted and
			// populate the manager with them,
			f = Path.Combine(basePath, RegisteredBlockServers);
			if (File.Exists(f)) {
				StreamReader reader = new StreamReader(f);
				string line;
				while ((line = reader.ReadLine()) != null) {
					int p = line.IndexOf(",");
					long guid = Int64.Parse(line.Substring(0, p));
					IServiceAddress addr = ServiceAddresses.ParseString(line.Substring(p + 1));
					AddRegisteredBlockService(guid, addr);
				}
				reader.Close();
			}

			// Read all the registered root servers that were last persisted and
			// populate the manager with them,
			f = Path.Combine(basePath, RegisteredRootServers);
			if (File.Exists(f)) {
				StreamReader reader = new StreamReader(f);
				string line;
				while ((line = reader.ReadLine()) != null) {
					IServiceAddress addr = ServiceAddresses.ParseString(line);
					AddRegisteredRootService(addr);
				}
				reader.Close();
			}

			// Read all the registered manager servers that were last persisted and
			// populate the manager with them,
			f = Path.Combine(basePath, RegisteredManagerServers);
			if (File.Exists(f)) {
				StreamReader reader = new StreamReader(f);
				string line;
				while ((line = reader.ReadLine()) != null) {
					IServiceAddress addr = ServiceAddresses.ParseString(line);
					AddRegisteredManagerService(addr);
				}
				reader.Close();
			}

			// Perform the initialization procedure (contacts the other managers and
			// syncs data).
			base.OnStart();
		}

		protected override void OnStop() {
			try {
				localDb.Stop();
			} finally {
				base.OnStop();
			}

		}
	}
}