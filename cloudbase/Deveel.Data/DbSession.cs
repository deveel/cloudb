using System;
using System.IO;

using Deveel.Data.Net;
using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbSession : IDisposable {
		private readonly int id;
		private readonly NetworkClient client;
		private readonly string path;
		private volatile bool checkDone;

		private static int SessionCounter = -1;

		public DbSession(NetworkClient client, string path) {
			this.client = client;
			this.path = path;
			id = SessionCounter++;
		}

		public string Path {
			get { return path; }
		}

		public NetworkClient Client {
			get { return client; }
		}

		private void CheckPath(ITransaction transaction) {
			if (!checkDone) {
				lock(this) {
					DataFile df = transaction.GetFile(DbTransaction.MagicKey, FileAccess.Read);
					Properties magic_set = new Properties(df);
					string type = magic_set.GetValue("type");
					string version = magic_set.GetValue("version");

					// Error if the data is incorrect,
					if (type == null || !type.Equals("BasePath")) {
						throw new ApplicationException("Path '" + path + "' is not valid.");
					}

					checkDone = true;
				}
			}
		}

		private DbTransaction CreateTransaction(DataAddress root) {
			// Turn it into a transaction object,
			ITransaction transaction = client.CreateTransaction(root);
			// Check the path is a valid SDBTransaction format,
			CheckPath(transaction);
			// Wrap it around an SDBTransaction object, and return it
			return new DbTransaction(this, root, transaction);
		}

		public DbTransaction CreateTransaction(DbRootAddress rootAddress) {
			// Check the root address session is the same as this object,
			if (!rootAddress.Session.Equals(this))
				throw new ApplicationException("The root address is not from this session");
			
			return CreateTransaction(rootAddress.Address);
		}

		public DbTransaction CreateTransaction() {
			return CreateTransaction(GetCurrentSnapshot());
		}

		public DbRootAddress GetCurrentSnapshot() {
			return new DbRootAddress(this, client.GetCurrentSnapshot(path));
		}

		public DbRootAddress[] GetHistoricalSnapshots(DateTime start, DateTime end) {
			DataAddress[] roots = client.GetHistoricalSnapshots(path, start, end);
			// Wrap the returned objects in SDBRootAddress,
			DbRootAddress[] dbRoots = new DbRootAddress[roots.Length];
			for (int i = 0; i < roots.Length; ++i) {
				dbRoots[i] = new DbRootAddress(this, roots[i]);
			}
			return dbRoots;
		}

		public override bool Equals(object obj) {
			DbSession session = obj as DbSession;
			if (session == null)
				return false;

			return id.Equals(session.id) &&
			       path == session.path;
		}

		public override int GetHashCode() {
			return path.GetHashCode() ^ id.GetHashCode();
		}

		public void Dispose() {
			client.Dispose();
		}
	}
}