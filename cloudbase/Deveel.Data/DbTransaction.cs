using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Net;
using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbTransaction : IPathTransaction {
		private readonly DbSession session;
		private readonly DataAddress baseRoot;
		private readonly ITransaction transaction;

		private readonly List<String> log;
		private bool invalidated;

		private readonly Dictionary<string, DbTable> table_map;

		private readonly Directory files;
		private readonly Directory tables;

		private List<string> fileToCopy;
		private List<String> tablesToCopy;

		internal static readonly Key MagicKey = new Key(0, 0, 0);

		private static readonly Key FilesProperties = new Key(0, 1, 13);
		private static readonly Key FilesNamemap = new Key(0, 1, 14);
		private static readonly Key FilesIndex = new Key(0, 1, 15);

		private static readonly Key TableSetProperties = new Key(0, 1, 18);
		private static readonly Key TableSetNamemap = new Key(0, 1, 19);
		private static readonly Key TableSetIndex = new Key(0, 1, 20);

		private static readonly Key TransactionLogKey = new Key(0, 1, 11);

		internal DbTransaction(DbSession session, DataAddress baseRoot, ITransaction transaction) {
			this.session = session;
			this.transaction = transaction;
			this.baseRoot = baseRoot;

			log = new List<string>();
			table_map = new Dictionary<string, DbTable>();

			files = new Directory(transaction, FilesProperties, FilesNamemap, FilesIndex, 0, 10);
			tables = new Directory(transaction, TableSetProperties, TableSetNamemap, TableSetIndex, 0, 11);
		}

		internal ITransaction Parent {
			get { return transaction; }
		}

		IPathContext IPathTransaction.Context {
			get { return session; }
		}

		public DbRootAddress BaseRoot {
			get { return new DbRootAddress(session, baseRoot); }
		}

		public long FileCount {
			get { return files.Count; }
		}
		
		public long TableCount {
			get { return tables.Count; }
		}

		private static void CheckName(string name) {
			// Sanity checks,
			if (name == null)
				throw new ArgumentNullException("name");

			int len = name.Length;
			if (len <= 0 || len > 1024)
				throw new ApplicationException("Invalid file name: " + name);

			for (int i = 0; i < len; ++i) {
				char c = name[i];
				//TODO: actually we should check if the character is of a defined
				//      unicode category and not simply if is it a Letter or Digit
				if (Char.IsWhiteSpace(c) || !Char.IsLetterOrDigit(c))
					throw new ApplicationException("Invalid file name: " + name);
			}
		}

		private void Invalidate() {
			invalidated = true;
		}

		internal void ReplayFileLogEntry(String entry, DbTransaction from_transaction) {
			// Get the operation type and operation code,
			char t = entry[0];
			char op = entry[1];
			String name = entry.Substring(2);
			// If this is a file operation,
			if (t == 'F') {
				if (op == 'C') {
					CreateFile(name);
				} else if (op == 'D') {
					DeleteFile(name);
				} else if (op == 'M') {
					if (fileToCopy == null) {
						fileToCopy = new List<string>();
					}
					// Copy the contents from the source if it hasn't already been
					// copied.
					if (!fileToCopy.Contains(name)) {
						from_transaction.files.CopyTo(name, files);
						fileToCopy.Add(name);
					}
					// Make sure to copy this event into the log in this transaction,
					log.Add(entry);
				} else {
					throw new ApplicationException("Transaction log entry error: " + entry);
				}
			} else {
				throw new ApplicationException("Transaction log entry error: " + entry);
			}
		}

		internal void ReplayTableLogEntry(string entry, DbTransaction srcTransaction, bool hasHistoricDataChanges) {
			char t = entry[0];
			char op = entry[1];
			String name = entry.Substring(2);
			// If this is a table operation,
			if (t == 'T') {
				if (op == 'C') {
					CreateTable(name);
				} else if (op == 'D') {
					DeleteTable(name);
				} else if (op == 'M' || op == 'S') {
					// If it's a TS event (a structural change to the table), we need to
					// pass this to the table merge function.
					bool structural_change = (op == 'S');
					// To replay a table modification
					if (tablesToCopy == null)
						tablesToCopy = new List<string>();

					if (!tablesToCopy.Contains(name)) {
						DbTable st = srcTransaction.GetTable(name);
						DbTable dt = GetTable(name);
						// Merge the source table into the destination table,
						dt.MergeFrom(st, structural_change, hasHistoricDataChanges);
						tablesToCopy.Add(name);
					}
					// Make sure to copy this event into the log in this transaction,
					log.Add(entry);
				} else {
					throw new ApplicationException("Transaction log entry error: " + entry);
				}
			} else {
				throw new ApplicationException("Transaction log entry error: " + entry);
			}
		}
		
		internal void CheckValid() {
			if (invalidated)
				throw new ApplicationException("Transaction has been invalidated");
		}
		
		internal void OnFileChanged(string fileName) {
			// Checks the log for any mutations on this filename, if not found adds the
			// mutation to the log.
			string logEntry = "FM" + fileName;
			if (!log.Contains(logEntry))
				log.Add(logEntry);
		}

		internal TextReader GetLogReader() {
			DataFile file = transaction.GetFile(TransactionLogKey, FileAccess.Read);
			return new StringDataReader(new StringData(file));
		}

		internal void RefreshLog() {
			// Output the change log to the proposed transaction to commit,
			DataFile file = transaction.GetFile(TransactionLogKey, FileAccess.Write);
			file.Delete();
			StringData logFile = new StringData(file);

			// Record the base root in the log,
			logFile.Append(baseRoot.ToString());
			logFile.Append("\n");

			// Write out every log entry,
			foreach (String entry in log) {
				logFile.Append(entry);
				logFile.Append("\n");
			}
		}

		private void ClearLog() {
			// Output the change log to the proposed transaction to commit,
			DataFile file = transaction.GetFile(TransactionLogKey, FileAccess.Write);
			file.Delete();
			StringData logFile = new StringData(file);

			// Record that there is no base root for this transaction,
			logFile.Append("no base root");
			logFile.Append("\n");
		}

		public IList<string> ListFiles() {
			CheckValid();
			return files.ListFiles();
		}

		public bool FileExists(string fileName) {
			CheckValid();
			CheckName(fileName);

			return files.GetFileKey(fileName) != null;
		}

		public bool DeleteFile(string fileName) {
			CheckValid();
			CheckName(fileName);

			Key k = files.GetFileKey(fileName);
			if (k == null)
				return false;

			files.DeleteFile(fileName);
			// Log this operation,
			log.Add("FD" + fileName);

			// File deleted so return success,
			return true;
		}

		public bool CreateFile(string fileName) {
			CheckValid();
			CheckName(fileName);

			Key k = files.GetFileKey(fileName);
			if (k != null)
				return false;

			files.CreateFile(fileName);
			// Log this operation,
			log.Add("FC" + fileName);

			// File created so return success,
			return true;
		}
		
		public IList<string> ListTables() {
			CheckValid();
			return tables.ListFiles();
		}
		
		public bool TableExists(string tableName) {
			CheckValid();
			CheckName(tableName);
			
			return tables.GetFileKey(tableName) != null;
		}
		
		public bool CreateTable(string tableName) {
			CheckValid();
			CheckName(tableName);

			Key k = tables.GetFileKey(tableName);
			if (k != null)
				return false;
			
			k = tables.CreateFile(tableName);
			
			long kid = k.Primary;
			if (kid > Int64.MaxValue)
				throw new ApplicationException("Id pool exhausted for table item.");
			
			// Log this operation,
			log.Add("TC" + tableName);
			
			// Table created so return success,
			return true;
		}
		
		public bool DeleteTable(string tableName) {
			CheckValid();
			CheckName(tableName);
			
			Key k = tables.GetFileKey(tableName);
			if (k == null)
				return false;
			
			// Fetch the table, and delete all data associated with it,
			DbTable table;
			lock (table_map) {
				table = GetTable(tableName);
				table_map.Remove(tableName);
			}
			
			table.DeleteFully();
			
			// Remove the item from the table directory,
			tables.DeleteFile(tableName);
			
			// Log this operation,
			log.Add("TD" + tableName);
			
			// Table deleted so return success,
			return true;
		}
		
		public DbTable GetTable(string tableName) {
			CheckValid();
			CheckName(tableName);
			
			// Is it in the map?
			lock (table_map) {
				DbTable table;
				if (table_map.TryGetValue(tableName, out table))
					return table;
				
				Key k = tables.GetFileKey(tableName);
				if (k == null)
					throw new ApplicationException("Table doesn't exist: " + tableName);
				
				long kid = k.Primary;
				if (kid > Int64.MaxValue)
					throw new ApplicationException("Id pool exhausted for table item.");
				
				// Turn the key into an SDBTable,
				table = new DbTable(this, tableName, tables.GetFile(tableName),(int) kid);
				table_map[tableName] = table;
				
				// And return it,
				return table;
			}
		}

		DataAddress IPathTransaction.Commit() {
			DbRootAddress address = Commit();
			return address.Address;
		}
		
		public DbRootAddress Commit() {
			CheckValid();
			
			try {
				// Update transaction information on the tables that were modified during
				// this transaction.
				lock (table_map) {
					foreach(KeyValuePair<string, DbTable> pair in table_map) {
						if (pair.Value.IsModified) {
							// Log this modification,
							// If there was a structural change to the table we log as a TS
							// event
							if (pair.Value.IsSchemaModified) {
								log.Add("TS" + pair.Key);
							} else {
								// Otherwise we log as a TM event (rows or columns were deleted).
								log.Add("TM" + pair.Key);
							}
							
							// Write out the table log
							pair.Value.PrepareCommit();
						}
					}
				}
				
				// Process the transaction log and write it out to a DataFile for the
				// path to handle,
				
				// If there are changes to commit,
				if (log.Count > 0) {
					// Refresh the transaction log with the entries stored in 'log'
					RefreshLog();

					// The database client,
					NetworkClient client = session.Client;
					
					// Flush the transaction to the network
					DataAddress proposal = client.FlushTransaction(transaction);
					
					// Perform the commit operation,
					return new DbRootAddress(session, client.Commit(session.PathName, proposal));
				} else {
					// No changes, so return base root
					return new DbRootAddress(session, baseRoot);
				}
			} finally {
				// Make sure transaction is invalidated
				Invalidate();
			}
		}
		
		 public DbRootAddress Publish(DbSession destSession) {
			// Refresh the transaction log with a cleared 'no base root' version.
			// This operation sets up the transaction log appropriately so that the
			// 'commit' process of future transactions will understand that this
			// version is not an iteration of previous versions.
			ClearLog();
			
			// The database client,
			NetworkClient client = session.Client;
			// Flush the transaction to the network
			DataAddress to_publish = client.FlushTransaction(transaction);
			
			try {
				DataAddress published = client.Commit(destSession.PathName, to_publish);
				// Return the root in the destionation session,
				return new DbRootAddress(destSession, published);
			} catch (CommitFaultException e) {
				// This shouldn't be thrown,
				throw new ApplicationException("Unexpected Commit Fault", e);
			}
		}

		
		public void Dispose() {
			if (session != null)
				session.Client.DisposeTransaction(transaction);
		}
	}
}