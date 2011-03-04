using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data {
	public class DbIndex : IEnumerable<DbRow> {
		private readonly DbTable table;
		private readonly long table_version;
		private readonly IIndexedObjectComparer<string> collator;
		private readonly long columnid;
		private readonly IIndex index;
		private readonly long start, end;
	
		private DbIndex(DbIndex copied) {
			table = copied.table;
			table_version = copied.table_version;
			collator = copied.collator;
			columnid = copied.columnid;
			index = copied.index;
			start = copied.start;
			end = copied.end;
		}
		
		internal DbIndex(DbTable table, long table_version, IIndexedObjectComparer<string> collator,
		                 long columnid, IIndex index, long start, long end) {
			this.table = table;
			this.table_version = table_version;
			this.collator = collator;
			this.columnid = columnid;
			this.index = index;
			if (start <= end) {
				this.start = start;
				this.end = end;
			} else {
				this.start = start;
				this.end = start;
			}
		}

		internal DbIndex(DbTable table, long table_version, IIndexedObjectComparer<string> collator,
		                 long columnid, IIndex index)
			: this(table, table_version, collator, columnid, index, 0, index.Count) {
		}

		public long Count {
			get {
				CheckVersion();
				return end - start;
			}
		}

		public virtual DbRow First {
			get {
				CheckVersion();
				if (start >= end)
					return null;
				long rowid = index[start];
				return new DbRow(table, rowid);
			}
		}

		public virtual DbRow Last {
			get {
				CheckVersion();
				if (start >= end)
					return null;
				long rowid = index[end - 1];
				return new DbRow(table, rowid);
			}
		}

		private void CheckVersion() {
			if (table_version != table.CurrentVersion)
				throw new InvalidOperationException();
		}

		private long StartPositionSearch(string s, bool inclusive) {
			long pos;
			if (!inclusive) {
				// Not inclusive
				pos = index.SearchLast(s, collator);
				if (pos < 0) {
					pos = -(pos + 1);
				} else {
					pos = pos + 1;
				}
			} else {
				// Inclusive
				pos = index.SearchFirst(s, collator);
				if (pos < 0) {
					pos = -(pos + 1);
				}
			}

			// If it's found, if the position is out of bounds then cap it,
			if (pos >= 0) {
				if (pos < start) {
					pos = start;
				}
				if (pos > end) {
					pos = end;
				}
			}
			return pos;
		}

		private long EndPositionSearch(String s, bool inclusive) {
			long pos;
			if (!inclusive) {
				// Not inclusive
				pos = index.SearchFirst(s, collator);
				if (pos < 0) {
					pos = -(pos + 1);
				}
			} else {
				// Inclusive
				pos = index.SearchLast(s, collator);
				if (pos < 0) {
					pos = -(pos + 1);
				} else {
					pos = pos + 1;
				}
			}

			// If it's found, if the position is out of bounds then cap it,
			if (pos >= 0) {
				if (pos < start) {
					pos = start;
				}
				if (pos > end) {
					pos = end;
				}
			}
			return pos;
		}

		public bool Contains(string s) {
			CheckVersion();

			// Find the start and end positions,
			long pos_s = index.SearchFirst(s, collator);
			if (pos_s >= 0) {
				if (pos_s >= end)
					return false;
				if (pos_s >= start)
					return true;

				// We are here if pos_s < start, so check if the last value is within the
				// bounds,
				long pos_e = index.SearchLast(s, collator);
				return pos_e >= start;
			}

			return false;
		}

		public virtual DbRowCursor GetCursor() {
			CheckVersion();
			return new DbRowCursor(table, table_version, index.GetCursor(start, end - 1));
		}
  
		IEnumerator<DbRow> IEnumerable<DbRow>.GetEnumerator() {
			return GetCursor();
		}
		
		IEnumerator IEnumerable.GetEnumerator() {
			return GetCursor();
		}

		public virtual DbIndex Reverse() {
			CheckVersion();
			return new ReverseIndex(this);
		}

		public virtual DbIndex Head(string toElement, bool inclusive) {
			CheckVersion();
			// Find the element position
			long pos = EndPositionSearch(toElement, inclusive);
			return new DbIndex(table, table_version, collator, columnid, index, start, pos);
		}

		public DbIndex Head(string toElement) {
			return Head(toElement, false);
		}

		public virtual DbIndex Tail(string fromElement, bool inclusive) {
			CheckVersion();
			// Find the element position
			long pos = StartPositionSearch(fromElement, inclusive);
			return new DbIndex(table, table_version, collator, columnid, index, pos, end);
		}

		public DbIndex Tail(string fromElement) {
			return Tail(fromElement, true);
		}

		public virtual DbIndex Sub(string fromElement, bool fromInclusive, string toElement, bool toInclusive) {
			CheckVersion();

			long nstart = StartPositionSearch(fromElement, fromInclusive);
			long nend = EndPositionSearch(toElement, toInclusive);

			return new DbIndex(table, table_version, collator, columnid, index, nstart, nend);
		}

		public DbIndex Sub(string fromElement, string toElement) {
			return Sub(fromElement, true, toElement, false);
		}

		#region ReverseIndex

		private sealed class ReverseIndex : DbIndex {

			private readonly DbIndex original;

			internal ReverseIndex(DbIndex backed)
				: base(backed) {
				original = backed;
			}

			public override DbIndex Reverse() {
				return original;
			}

			public override DbRow First {
				get { return base.Last; }
			}

			public override DbRow Last {
				get { return base.First; }
			}

			public override DbIndex Head(string toElement, bool inclusive) {
				return new ReverseIndex(base.Tail(toElement, inclusive));
			}

			public override DbIndex Tail(String fromElement, bool inclusive) {
				return new ReverseIndex(base.Head(fromElement, inclusive));
			}

			public override DbIndex Sub(string fromElement, bool fromInclusive, string toElement, bool toInclusive) {
				return new ReverseIndex(base.Sub(toElement, toInclusive, fromElement, fromInclusive));
			}

			public override DbRowCursor GetCursor() {
				return new DbRowCursor(base.table, base.table_version, new ReverseRowCursor(base.index.GetCursor(start, base.end - 1)));
			}

			#region ReverseRowCursor

			private class ReverseRowCursor : IIndexCursor {
				private readonly IIndexCursor backed;

				public ReverseRowCursor(IIndexCursor backed) {
					this.backed = backed;
					this.backed.Position = this.backed.Count;
				}

				public void Dispose() {
				}

				public bool MoveNext() {
					return backed.MoveBack();
				}

				public void Reset() {
					backed.Position = backed.Count;
				}

				public long Current {
					get { return backed.Count; }
				}

				object IEnumerator.Current {
					get { return Current; }
				}

				public object Clone() {
					throw new NotSupportedException();
				}

				public long Count {
					get { return backed.Count; }
				}

				public long Position {
					get {
						long size = backed.Count;
						return size - (backed.Position + 1);
					}
					set {
						long size = backed.Count;
						backed.Position = (size - (value + 1));
					}
				}

				public bool MoveBack() {
					return backed.MoveNext();
				}

				public long Remove() {
					throw new NotSupportedException();
				}
			}

			#endregion

		}

		#endregion
	}
}