using System;
using System.Collections.Generic;

namespace Deveel.Data {
	[DbTrusted]
	public sealed class DbRowCursor : IEnumerator<DbRow> {
		private readonly DbTable table;
		private readonly long tableVersion;
		private readonly IIndexCursor cursor;
		
		internal DbRowCursor(DbTable table, long tableVersion, IIndexCursor cursor) {
			this.cursor = cursor;
			this.table = table;
			this.tableVersion = tableVersion;
		}
		
		public long Count {
			get {
				CheckVersion();
				return cursor.Count;
			}
		}
		
		public long Position {
			get{
				CheckVersion();
				return cursor.Position;
			}
			set{
				CheckVersion();
				cursor.Position = value;
			}
		}
		
		public DbRow Current {
			get {
				CheckVersion();
				return new DbRow(table, cursor.Current);
			}
		}

		public long CurrentRowId {
			get {
				CheckVersion();
				return cursor.Current;
			}
		}
		
		object System.Collections.IEnumerator.Current {
			get { return Current; }
		}
		
		private void CheckVersion() {
			if (tableVersion != table.CurrentVersion)
				throw new InvalidOperationException("The source table has been modified.");
		}
		
		public void Dispose() {
		}
		
		public bool MoveNext() {
			CheckVersion();
			return cursor.MoveNext();
		}
		
		public bool MoveBack() {
			CheckVersion();
			return cursor.MoveBack();
		}
		
		public void Reset() {
			CheckVersion();
			cursor.Reset();
		}
		
		public void Remove() {
			CheckVersion();
			cursor.Remove();
		}
	}
}