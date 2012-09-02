using System;
using System.IO;
using System.Threading;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class ShardedDataTestBase {
		private readonly StoreType storeType;
		private IDatabase database;
		private ITransaction transaction;
		private string path;

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);

		protected ShardedDataTestBase(StoreType storeType) {
			this.storeType = storeType;
		}

		protected StoreType StoreType {
			get { return storeType; }
		}

		protected string TestPath {
			get { return path ?? (path = Path.Combine(Environment.CurrentDirectory, "base")); }
		}

		protected ITransaction Transaction {
			get { return transaction; }
		}

		protected virtual IDatabase CreateDatabase() {
			if (storeType == StoreType.Memory)
				return new MemoryDatabase(1024);

			if (storeType == StoreType.FileSystem) {
				string testPath = TestPath;
				if (Directory.Exists(testPath))
					Directory.Delete(testPath);

				Directory.CreateDirectory(testPath);
				return new FileSystemDatabase(testPath);
			}

			throw new NotSupportedException();
		}

		protected void Commit() {
			database.Publish(transaction);
		}

		[SetUp]
		public void SetUp() {
			SetupEvent.WaitOne();

			database = CreateDatabase();

			if (storeType == StoreType.FileSystem)
				((FileSystemDatabase) database).Start();
			else {
				((MemoryDatabase)database).Start();
			}

			transaction = database.CreateTransaction();
		}

		[TearDown]
		public virtual void TearDown() {
			try {
				if (storeType == StoreType.FileSystem) {
					((FileSystemDatabase)database).Stop();
				} else {
					((MemoryDatabase)database).Stop();
				}
			} finally {
				SetupEvent.Set();
			}
		}
	}
}