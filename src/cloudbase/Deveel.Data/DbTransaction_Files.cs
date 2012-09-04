using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public sealed partial class DbTransaction {
		private readonly Directory fileSet;
		private List<string> fileCopySet;

		private static readonly Key FileSetProperties = new Key(0, 1, 13);
		private static readonly Key FileSetNamemap = new Key(0, 1, 14);
		private static readonly Key FileSetIndex = new Key(0, 1, 15);

		public long FileCount {
			get { return fileSet.Count; }
		}

		public IList<string> Files {
			get {
				CheckValid();
				return fileSet.Items;
			}
		}


		internal void LogFileChange(string fileName) {
			// Checks the log for any mutations on this filename, if not found adds the
			// mutation to the log.
			string mutationLogEntry = "FM" + fileName;
			foreach (String entry in log) {
				if (entry.Equals(mutationLogEntry)) {
					// Found so return,
					return;
				}
			}
			// Not found, so add it to the end
			log.Add(mutationLogEntry);
		}

		internal void ReplayFileLogEntry(string entry, DbTransaction srcTransaction) {
			// Get the operation type and operation code,
			char t = entry[0];
			char op = entry[1];
			string name = entry.Substring(2);
			// If this is a file operation,
			if (t != 'F')
				throw new ApplicationException("Transaction log entry error: " + entry);

			if (op == 'C') {
				CreateFile(name);
			} else if (op == 'D') {
				DeleteFile(name);
			} else if (op == 'M') {
				if (fileCopySet == null)
					fileCopySet = new List<string>();

				// Copy the contents from the source if it hasn't already been
				// copied.
				if (!fileCopySet.Contains(name)) {
					srcTransaction.fileSet.CopyTo(name, fileSet);
					fileCopySet.Add(name);
				}
				// Make sure to copy this event into the log in this transaction,
				log.Add(entry);
			} else {
				throw new ApplicationException("Transaction log entry error: " + entry);
			}
		}

		public bool FileExists(string fileName) {
			CheckValid();
			CheckNameValid(fileName);

			return fileSet.GetItem(fileName) != null;
		}

		public bool CreateFile(string fileName) {
			CheckValid();
			CheckNameValid(fileName);

			Key k = fileSet.GetItem(fileName);
			if (k != null)
				return false;

			k = fileSet.AddItem(fileName);
			// Log this operation,
			log.Add("FC" + fileName);

			// File created so return success,
			return true;
		}

		public bool DeleteFile(string fileName) {
			CheckValid();
			CheckNameValid(fileName);

			Key k = fileSet.GetItem(fileName);
			if (k == null)
				return false;

			fileSet.RemoveItem(fileName);
			// Log this operation,
			log.Add("FD" + fileName);

			// File deleted so return success,
			return true;
		}

		public DbFile GetFile(string fileName) {
			CheckValid();
			CheckNameValid(fileName);

			Key k = fileSet.GetItem(fileName);
			if (k == null)
				// Doesn't exist, so throw an exception
				throw new ApplicationException("File doesn't exist: " + fileName);

			// Wrap the object in order to capture update events,
			return new DbFile(this, fileName, fileSet.GetItemDataFile(fileName));

		}
	}
}