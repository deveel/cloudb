using System;
using System.Collections.Generic;

namespace Deveel.Data {
	[Trusted]
	public sealed class DbRow {
		private readonly DbTable table;
		private readonly long rowid;

		private readonly Dictionary<string, string> values;

		internal DbRow(DbTable table, long rowid) {
			this.table = table;
			this.rowid = rowid;

			if (IsNew)
				values = new Dictionary<string, string>();
		}

		internal long RowId {
			get { return rowid; }
		}

		public bool IsNew {
			get { return rowid == -1; }
		}

		public string this[string columnName] {
			get { return GetValue(columnName); }
		}

		internal void ClearValues() {
			values.Clear();
		}

		internal string GetValue(long columnid) {
			return table.GetValue(rowid, columnid);
		}

		public string GetValue(string columnName) {
			if (IsNew) {
				string value;
				return values.TryGetValue(columnName, out value) ? value : null;
			}

			return GetValue(table.GetColumnId(columnName));
		}

		public void SetValue(string columnName, string value) {
			if (!IsNew)
				throw new NotSupportedException();

			if (table.GetColumnId(columnName) == -1)
				throw new ApplicationException("Column " + columnName + " was not found in table.");

			values[columnName] = value;
		}

		public void PreFetchValue(string columnName) {
			table.PreFetchValue(rowid, -1);
		}
	}
}