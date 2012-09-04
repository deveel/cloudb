using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public sealed partial class DbTransaction {
		private readonly Dictionary<string, DbTable> tableMap;
		private readonly Directory tableSet;

		private List<string> tableCopySet;

		private static readonly Key TableSetProperties = new Key(0, 1, 18);
		private static readonly Key TableSetNamemap = new Key(0, 1, 19);
		private static readonly Key TableSetIndex = new Key(0, 1, 20);

		public long TableCount {
			get { return tableSet.Count; }
		}

		public IList<string> TableNames {
			get {
				CheckValid();
				return tableSet.Items;
			}
		}

		internal void ReplayTableLogEntry(string entry, DbTransaction srcTransaction, bool historicChanges) {
			char t = entry[0];
			char op = entry[1];
			String name = entry.Substring(2);
			// If this is a table operation,
			if (t != 'T')
				throw new ApplicationException("Transaction log entry error: " + entry);

			if (op == 'C') {
				CreateTable(name);
			} else if (op == 'D') {
				DeleteTable(name);
			} else if (op == 'M' || op == 'S') {
				// If it's a TS event (a structural change to the table), we need to
				// pass this to the table merge function.
				bool structuralChange = (op == 'S');
				// To replay a table modification
				if (tableCopySet == null)
					tableCopySet = new List<string>();

				if (!tableCopySet.Contains(name)) {
					DbTable st = srcTransaction.GetTable(name);
					DbTable dt = GetTable(name);
					// Merge the source table into the destination table,
					dt.MergeFrom(st, structuralChange, historicChanges);
					tableCopySet.Add(name);
				}
				// Make sure to copy this event into the log in this transaction,
				log.Add(entry);
			} else {
				throw new ApplicationException("Transaction log entry error: " + entry);
			}
		}

		public bool TableExists(string tableName) {
			CheckValid();
			CheckNameValid(tableName);

			return tableSet.GetItem(tableName) != null;
		}

		public bool CreateTable(string tableName) {
			CheckValid();
			CheckNameValid(tableName);

			Key k = tableSet.GetItem(tableName);
			if (k != null) {
				return false;
			}

			k = tableSet.AddItem(tableName);

			long kid = k.Primary;
			if (kid > Int64.MaxValue) {
				// We ran out of keys so can't make any more table items,
				// This happens after 2 billion tables created. We need to note this
				// as a limitation.
				throw new ApplicationException("Id pool exhausted for table item.");
			}

			// Log this operation,
			log.Add("TC" + tableName);

			// Table created so return success,
			return true;
		}

		public bool DeleteTable(String tableName) {
			CheckValid();
			CheckNameValid(tableName);

			Key k = tableSet.GetItem(tableName);
			if (k == null)
				return false;

			// Fetch the table, and delete all data associated with it,
			DbTable table;
			lock (tableMap) {
				table = GetTable(tableName);
				tableMap.Remove(tableName);
			}

			table.DeleteFully();

			// Remove the item from the table directory,
			tableSet.RemoveItem(tableName);

			// Log this operation,
			log.Add("TD" + tableName);

			// Table deleted so return success,
			return true;
		}

		public DbTable GetTable(string tableName) {
			CheckValid();
			CheckNameValid(tableName);

			// Is it in the map?
			lock (tableMap) {
				DbTable table;
				if (tableMap.TryGetValue(tableName, out table))
					// It's there, so return it,
					return table;

				Key k = tableSet.GetItem(tableName);
				if (k == null)
					// Doesn't exist, so throw an exception
					throw new ApplicationException("Table doesn't exist: " + tableName);

				long kid = k.Primary;
				if (kid > Int64.MaxValue)
					// We ran out of keys so can't make any more table items,
					// This happens after 2 billion tables created. We need to note this
					// as a limitation.
					throw new ApplicationException("Id pool exhausted for table item.");

				// Turn the key into an SDBTable,
				table = new DbTable(this, tableSet.GetItemDataFile(tableName), (int) kid);
				tableMap[tableName] = table;
				// And return it,
				return table;
			}
		}
	}
}