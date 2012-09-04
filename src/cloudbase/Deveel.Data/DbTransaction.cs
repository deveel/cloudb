using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Net;

namespace Deveel.Data {
	public sealed partial class DbTransaction : IDisposable {
		private readonly DbSession session;
		private readonly DataAddress baseRoot;
		private readonly ITransaction transaction;

		private readonly List<String> log;

		private bool invalidated;


		internal static readonly Key MagicKey = new Key(0, 0, 0);

		private static readonly Key TransactionLogKey = new Key(0, 1, 11);


		internal DbTransaction(DbSession session, DataAddress baseRoot, ITransaction transaction) {
			this.session = session;
			this.baseRoot = baseRoot;
			this.transaction = transaction;

			log = new List<string>();
			tableMap = new Dictionary<string, DbTable>();

			fileSet = new Directory(transaction, FileSetProperties, FileSetNamemap, FileSetIndex, 0, 10);
			tableSet = new Directory(transaction, TableSetProperties, TableSetNamemap, TableSetIndex, 0, 11);
		}

		~DbTransaction() {
			Dispose(false);
		}

		internal ITransaction Transaction {
			get { return transaction; }
		}

		public DbRootAddress BaseRoot {
			get { return new DbRootAddress(session, baseRoot); }
		}

		public string PathName {
			get { return session.PathName; }
		}

		private void Invalidate() {
			invalidated = true;
		}

		internal void CheckValid() {
			if (invalidated)
				throw new ApplicationException("Transaction has been invalidated");
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				// For a network tree system, this isn't really necessary,
				session.Client.DisposeTransaction(transaction);
			}
		}

		private void CheckNameValid(String name) {
			// Sanity checks,
			if (name == null)
				throw new ArgumentNullException("name");

			int len = name.Length;
			if (len <= 0 || len > 1024)
				throw new ApplicationException("Invalid file name: " + name);

			for (int i = 0; i < len; ++i) {
				char c = name[i];
				// TODO: check i the character is defined n the Unicode table
				if (Char.IsWhiteSpace(c)) {
					throw new ApplicationException("Invalid file name: " + name);
				}
			}
		}

		internal TextReader GetLogReader() {
			IDataFile transactionLog = transaction.GetFile(TransactionLogKey, FileAccess.Read);
			StringData logFile = new StringData(transactionLog);
			return new StringDataReader(logFile);
		}

		internal void RefreshTransactionLog() {
			// Output the change log to the proposed transaction to commit,
			IDataFile transactionLog = transaction.GetFile(TransactionLogKey, FileAccess.ReadWrite);
			transactionLog.Delete();

			StringData logFile = new StringData(transactionLog);

			// Record the base root in the log,
			logFile.Append(baseRoot.ToString());
			logFile.Append("\n");

			// Write out every log entry,
			foreach (string entry in log) {
				logFile.Append(entry);
				logFile.Append("\n");
			}
		}

		internal static void WriteForcedTransactionIntroduction(ITransaction transaction) {
			// Output the change log to the proposed transaction to commit,
			IDataFile transactionLog = transaction.GetFile(TransactionLogKey, FileAccess.ReadWrite);
			transactionLog.Delete();

			StringData logFile = new StringData(transactionLog);

			// Record that there is no base root for this transaction,
			logFile.Append("no base root");
			logFile.Append("\n");
		}

		private void WriteClearedTransactionLog() {
			WriteForcedTransactionIntroduction(transaction);
		}

		public DbRootAddress Commit() {
			CheckValid();

			try {
				// Update transaction information on the tables that were modified during
				// this transaction.
				lock (tableMap) {
					foreach (KeyValuePair<string, DbTable> pair in tableMap) {
						string tableName = pair.Key;
						DbTable table = pair.Value;
						if (table.Modified) {
							// Log this modification,
							// If there was a structural change to the table we log as a TS
							// event
							if (table.HasStructuralChanges) {
								log.Add("TS" + tableName);
							}
								// Otherwise we log as a TM event (rows or columns were deleted).
							else {
								log.Add("TM" + tableName);
							}
							// Write out the table log
							table.PrepareForCommit();
						}
					}
				}

				// Process the transaction log and write it out to a DataFile for the
				// path function processor to handle,

				// If there are changes to commit,
				if (log.Count > 0) {

					// Refresh the transaction log with the entries stored in 'log'
					RefreshTransactionLog();

					// The database client,
					NetworkClient dbClient = session.Client;

					// Flush the transaction to the network
					DataAddress proposal = dbClient.FlushTransaction(transaction);
					// Perform the commit operation,
					return new DbRootAddress(session, dbClient.Commit(session.PathName, proposal));
				} else {
					// No changes, so return base root
					return new DbRootAddress(session, baseRoot);
				}

			}
				// Make sure transaction is invalidated
			finally {
				// Invalidate this transaction
				Invalidate();
			}
		}

		public DbRootAddress PublishToSession(DbSession destSession) {
			// Refresh the transaction log with a cleared 'no base root' version.
			// This operation sets up the transaction log appropriately so that the
			// 'commit' process of future transactions will understand that this
			// version is not an iteration of previous versions.
			WriteClearedTransactionLog();

			// The database client,
			NetworkClient dbClient = session.Client;
			// Flush the transaction to the network
			DataAddress toPublish = dbClient.FlushTransaction(transaction);

			try {
				DataAddress published = dbClient.Commit(destSession.PathName, toPublish);
				// Return the root in the destionation session,
				return new DbRootAddress(destSession, published);
			} catch (CommitFaultException e) {
				// This shouldn't be thrown,
				throw new ApplicationException("Unexpected Commit Fault", e);
			}
		}


		public void Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}
	}
}