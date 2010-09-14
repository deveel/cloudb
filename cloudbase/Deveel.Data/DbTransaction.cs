using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Net;
using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbTransaction : IDisposable {
		private readonly DbSession session;
		private readonly DataAddress baseRoot;
		private readonly ITransaction transaction;

		private readonly List<String> log;
		private bool invalidated;

		private readonly Dictionary<string, DbTable> table_map;

		private readonly Directory files;
		private readonly Directory tables;

		private List<string> fileToCopy;

		internal static readonly Key MagicKey = new Key(0, 0, 0);

		private static readonly Key FilesProperties = new Key(0, 1, 13);
		private static readonly Key FilesNamemap = new Key(0, 1, 14);
		private static readonly Key FilesIndex = new Key(0, 1, 15);

		private static readonly Key TableSetProperties = new Key(0, 1, 18);
		private static readonly Key TableSetNamemap = new Key(0, 1, 19);
		private static readonly Key TableSetIndex = new Key(0, 1, 20);

		private static readonly Key TransactionLogKey = new Key((short)0, 1, 11);

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

		public DbRootAddress BaseRoot {
			get { return new DbRootAddress(session, baseRoot); }
		}

		public long FileCount {
			get { return files.Count; }
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
			if (k != null) {
				return false;
			}

			k = files.CreateFile(fileName);
			// Log this operation,
			log.Add("FC" + fileName);

			// File created so return success,
			return true;

		}
		
		public void Dispose() {
			session.Client.DisposeTransaction(transaction);
		}
	}
}