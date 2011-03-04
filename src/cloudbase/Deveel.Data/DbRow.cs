using System;
using System.Collections.Generic;

namespace Deveel.Data {
	[DbTrusted]
	public sealed class DbRow {
		private readonly DbTable table;
		private long rowId;
		
		private readonly Dictionary<string, string> buffer = new Dictionary<string, string>();
		private readonly List<string> columnsToRemove = new List<string>();
		private bool dirty;
		
		internal DbRow(DbTable table, long rowId) {
			this.table = table;
			this.rowId = rowId;
		}
		
		internal long RowId {
			get { return rowId; }
			set { rowId = value; }
		}
		
		internal bool IsDirty {
			get { return dirty; }
			set { dirty = value; }
		}
				
		public string this[string columnName] {
			get { return GetValue(columnName); }
			set { SetValue(columnName, value); }
		}
		
		internal string GetValue(long columnId) {
			return table.GetCellValue(rowId, columnId);
		}
		
		public string GetValue(string columnName) {
			string value = null;
			if (dirty && buffer.ContainsKey(columnName)) {
				value = buffer[columnName];
			} else if (!columnsToRemove.Contains(columnName)) {
				value = GetValue(table.Schema.GetColumnId(columnName));
			}
			
			return value;
		}
		
		public void SetValue(string columnName, string value) {
			if (value != null) {
				buffer[columnName] = value;
			} else {
				if (buffer.Remove(columnName))
					columnsToRemove.Add(columnName);
			}
			
			dirty = true;
		}
	}
}