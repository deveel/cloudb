using System;

using Deveel.Console;

namespace Deveel.Data.Net {
	internal class NetworkContext : IExecutionContext {
		private readonly NetworkProfile netProfile;
		private string pathName;

		public NetworkContext(NetworkProfile netProfile) {
			this.netProfile = netProfile;
		}

		public void Dispose() {
		}

		public PropertyRegistry Properties {
			get { return null; }
		}

		public bool IsIsolated {
			get { return true; }
		}

		public NetworkProfile Network {
			get { return netProfile; }
		}

		public string PathName {
			get { return pathName; }
			set { pathName = value; }
		}

		public void CreateTable(string path, string table, string[] columns) {
			CreateTable(path, table, columns, new string[0]);
		}

		public void CreateTable(string path, string table, string[] columns, string[] indexedColumns) {
			throw new NotImplementedException();
		}

		public void AddValueToPath(string path, string tableName, string key, string value) {
			if (!BasePathWrapper.IsSupported)
				throw new ApplicationException("Base path is not supported.");

			MachineProfile manager = netProfile.ManagerServer;
			if (manager == null)
				throw new ApplicationException();

			NetworkClient client = new NetworkClient(manager.Address, netProfile.Connector);
			BasePathWrapper wrapper = new BasePathWrapper();
			object session = wrapper.CreateDbSession(client, path);

			using (IDisposable transaction = wrapper.CreateDbTransaction(session) as IDisposable) {
				if (!wrapper.TableExists(transaction, tableName))
					throw new ApplicationException();

				wrapper.Insert(transaction, tableName, key, value);
				wrapper.Commit(transaction);
			}
		}

		public void AddValueToPath(string tableName, string key, string value) {
			if (String.IsNullOrEmpty(pathName))
				throw new ApplicationException("The default path was not set.");

			AddValueToPath(pathName, tableName, key, value);
		}

		public void CreatTable(string path, string tableName, string [] columns, string [] indexedColumns) {
			if (!BasePathWrapper.IsSupported)
				throw new ApplicationException("Base path is not supported.");

			MachineProfile manager = netProfile.ManagerServer;
			if (manager == null)
				throw new ApplicationException();

			NetworkClient client = new NetworkClient(manager.Address, netProfile.Connector);
			BasePathWrapper wrapper = new BasePathWrapper();
			object session = wrapper.CreateDbSession(client, path);

			using (IDisposable transaction = wrapper.CreateDbTransaction(session) as IDisposable) {
				if (!wrapper.TableExists(transaction, tableName)) {
					wrapper.CreateTable(transaction, tableName, columns, indexedColumns);
					wrapper.Commit(transaction);
				}
			}
		}
	}
}