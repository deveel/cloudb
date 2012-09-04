using System;
using System.IO;

using Deveel.Data.Net;

namespace Deveel.Data {
	public sealed class DbSession {
		private readonly NetworkClient client;
		private readonly string pathName;
		private volatile bool checkPerformed;

		public DbSession(NetworkClient client, string pathName) {
			this.client = client;
			this.pathName = pathName;
		}

		public NetworkClient Client {
			get { return client; }
		}

		public string PathName {
			get { return pathName; }
		}

		private void CheckPathValid(ITransaction transaction) {
			if (!checkPerformed) {
				lock (this) {
					if (!checkPerformed) {
						IDataFile df = transaction.GetFile(DbTransaction.MagicKey, FileAccess.Read);
						StringDictionary magicSet = new StringDictionary(df);
						string obType = magicSet["ob_type"];
						string version = magicSet["version"];

						// Error if the data is incorrect,
						if (obType == null || !obType.Equals("Deveel.Data.CloudBase"))
							throw new ApplicationException("Path '" + PathName + "' is not a CloudBase");

						checkPerformed = true;
					}
				}
			}
		}

		private DbTransaction CreateTransaction(DataAddress baseRoot) {
			// Turn it into a transaction object,
			ITransaction transaction = client.CreateTransaction(baseRoot);
			// Check the path is a valid DbTransaction format,
			CheckPathValid(transaction);
			// Wrap it around an DbTransaction object, and return it
			return new DbTransaction(this, baseRoot, transaction);
		}

		public DbTransaction CreateTransaction() {
			return CreateTransaction(GetCurrentSnapshot());
		}

		private DbRootAddress GetCurrentSnapshot() {
			return new DbRootAddress(this, client.GetCurrentSnapshot(PathName));

		}

		public DbTransaction CreateTransaction(DbRootAddress rootAddress) {
			// Check the root address session is the same as this object,
			if (!rootAddress.Session.Equals(this))
				throw new ApplicationException("root_address is not from this session");

			return CreateTransaction(rootAddress.Address);
		}

		public DbRootAddress[] GetHistoricalSnapshots(DateTime timeStart, DateTime timeEnd) {
			DataAddress[] roots = client.GetHistoricalSnapshots(PathName, timeStart, timeEnd);
			// Wrap the returned objects in SDBRootAddress,
			DbRootAddress[] sdbRoots = new DbRootAddress[roots.Length];
			for (int i = 0; i < roots.Length; ++i) {
				sdbRoots[i] = new DbRootAddress(this, roots[i]);
			}
			return sdbRoots;
		}

		public override bool Equals(object obj) {
			if (ReferenceEquals(null, obj))
				return false;
			if (ReferenceEquals(this, obj))
				return true;
			return obj is DbSession && Equals((DbSession) obj);
		}

		private bool Equals(DbSession other) {
			return client.Equals(other.client) && string.Equals(pathName, other.pathName);
		}

		public override int GetHashCode() {
			unchecked {
				return (client.GetHashCode() * 397) ^ pathName.GetHashCode();
			}
		}
	}
}