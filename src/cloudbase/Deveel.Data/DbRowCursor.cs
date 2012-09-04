using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data {
	public sealed class DbRowCursor : IEnumerator<DbRow> {
		private readonly DbTable table;
		private long tableVersion;
		private readonly IIndexCursor cursor;

		internal DbRowCursor(DbTable table, long tableVersion, IIndexCursor cursor) {
			this.table = table;
			this.tableVersion = tableVersion;
			this.cursor = cursor;
		}

		private void VersionCheck() {
			if (tableVersion != table.CurrentVersion)
				throw new InvalidOperationException();
		}

		public long Count {
			get {
				VersionCheck();
				return cursor.Count;
			}
		}

		public long Position {
			get {
				VersionCheck();
				return cursor.Position;
			}
			set {
				VersionCheck();
				cursor.Position = value;
			}
		}

		public void Dispose() {
			cursor.Dispose();
		}

		public bool MoveNext() {
			VersionCheck();
			return cursor.MoveNext();
		}

		public bool MoveBack() {
			VersionCheck();
			return cursor.MoveBack();
		}

		public void Reset() {
			tableVersion = table.CurrentVersion;
			cursor.Reset();
		}

		public DbRow Current {
			get {
				VersionCheck();
				long rowid = cursor.Current;
				return new DbRow(table, rowid);
			}
		}

		object IEnumerator.Current {
			get { return Current; }
		}
	}
}