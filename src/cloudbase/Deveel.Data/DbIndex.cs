using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data {
	[Trusted]
	public class DbIndex : IEnumerable<DbRow> {
		private readonly DbTable table;
		private readonly long tableVersion;
		private readonly IIndexedObjectComparer<string> collator;
		private readonly long columnid;
		private readonly IIndex index;
		private readonly long start, end;

		internal DbIndex(DbIndex source) {
			table = source.table;
			tableVersion = source.tableVersion;
			collator = source.collator;
			columnid = source.columnid;
			index = source.index;
			start = source.start;
			end = source.end;
		}

		internal DbIndex(DbTable table, long tableVersion, IIndexedObjectComparer<string> collator, long columnid, IIndex index) 
			: this(table, tableVersion, collator, columnid, index, 0, index.Count) {
		}

		internal DbIndex(DbTable table, long tableVersion, IIndexedObjectComparer<string> collator, long columnid, IIndex index, long start, long end) {
			this.table = table;
			this.tableVersion = tableVersion;
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

		public long Count {
			get {
				VersionCheck();
				return end - start;
			}
		}

		public virtual DbRow First {
			get {
				VersionCheck();
				// Return null if empty
				if (start >= end)
					return null;

				long rowid = index[start];
				return new DbRow(table, rowid);
			}
		}

		public virtual DbRow Last {
			get {
				VersionCheck();
				// Return null if empty
				if (start >= end)
					return null;

				long rowid = index[end - 1];
				return new DbRow(table, rowid);
			}
		}

		private long StartPositionSearch(String e, bool inclusive) {
			long pos;
			if (!inclusive) {
				// Not inclusive
				pos = index.SearchLast(e, collator);
				if (pos < 0) {
					pos = -(pos + 1);
				} else {
					pos = pos + 1;
				}
			} else {
				// Inclusive
				pos = index.SearchFirst(e, collator);
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

		private long EndPositionSearch(string e, bool inclusive) {
			long pos;
			if (!inclusive) {
				// Not inclusive
				pos = index.SearchFirst(e, collator);
				if (pos < 0) {
					pos = -(pos + 1);
				}
			} else {
				// Inclusive
				pos = index.SearchLast(e, collator);
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

		private void VersionCheck() {
			if (tableVersion != table.CurrentVersion)
				throw new InvalidOperationException();
		}

		public bool Contains(string e) {
			VersionCheck();

			// Find the start and end positions,
			long posStart = index.SearchFirst(e, collator);
			if (posStart >= 0) {
				if (posStart >= end)
					return false;
				if (posStart >= start)
					return true;

				// We are here if pos_s < start, so check if the last value is within the
				// bounds,
				long posEnd = index.SearchLast(e, collator);
				return posEnd >= start;
			}
			// Not found, return false
			return false;
		}

		public virtual DbRowCursor GetCursor() {
			VersionCheck();
			return new DbRowCursor(table, tableVersion, index.GetCursor(start, end - 1));
		}

		IEnumerator<DbRow> IEnumerable<DbRow>.GetEnumerator() {
			return GetCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetCursor();
		}

		public virtual DbIndex Reverse() {
			VersionCheck();
			return new ReverseIndex(this);
		}

		public DbIndex Head(string toElement) {
			return Head(toElement, false);
		}

		public virtual DbIndex Head(string toElement, bool inclusive) {
			VersionCheck();
			// Find the element position
			long pos = EndPositionSearch(toElement, inclusive);
			return new DbIndex(table, tableVersion, collator, columnid, index, start, pos);
		}

		public DbIndex Tail(string fromElement) {
			return Tail(fromElement, true);
		}

		public virtual DbIndex Tail(string fromElement, bool inclusive) {
			VersionCheck();
			// Find the element position
			long pos = StartPositionSearch(fromElement, inclusive);
			return new DbIndex(table, tableVersion, collator, columnid, index, pos, end);
		}

		public DbIndex Sub(string fromElement, string toElement) {
			return Sub(fromElement, true, toElement, false);
		}

		public virtual DbIndex Sub(string fromElement, bool fromInclusive, string toElement, bool toInclusive) {
			VersionCheck();

			long nstart = StartPositionSearch(fromElement, fromInclusive);
			long nend = EndPositionSearch(toElement, toInclusive);

			return new DbIndex(table, tableVersion, collator, columnid, index, nstart, nend);
		}

		#region ReverseIndex

		class ReverseIndex : DbIndex {
			private readonly DbIndex source;

			public ReverseIndex(DbIndex source)
				: base(source) {
				this.source = source;
			}

			public override DbRow First {
				get { return source.Last; }
			}

			public override DbRow Last {
				get { return source.First; }
			}

			public override DbIndex Reverse() {
				return source;
			}

			public override DbIndex Head(string toElement, bool inclusive) {
				return new ReverseIndex(base.Tail(toElement, inclusive));
			}

			public override DbIndex Tail(string fromElement, bool inclusive) {
				return new ReverseIndex(base.Head(fromElement, inclusive));
			}

			public override DbIndex Sub(string fromElement, bool fromInclusive, string toElement, bool toInclusive) {
				return new ReverseIndex(base.Sub(toElement, toInclusive, fromElement, fromInclusive));
			}

			public override DbRowCursor GetCursor() {
				return new DbRowCursor(table, tableVersion, new ReverseCursor(index.GetCursor(start, end - 1)));
			}
		}

		#endregion

		#region ReverseCursor

		internal class ReverseCursor : IIndexCursor {
			private readonly IIndexCursor source;

			public ReverseCursor(IIndexCursor source) {
				this.source = source;
				this.source.Position = source.Count;
			}

			public void Dispose() {
				throw new NotImplementedException();
			}

			public bool MoveNext() {
				return source.MoveBack();
			}

			public void Reset() {
				throw new NotImplementedException();
			}

			public long Current {
				get { return source.Current; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public object Clone() {
				throw new NotSupportedException();
			}

			public long Count {
				get { return source.Count; }
			}

			public long Position {
				get {
					long size = source.Count;
					return size - (source.Position + 1);
				} 
				set {
					long size = source.Count;
					source.Position = (size - (value + 1));
				}
			}

			public bool MoveBack() {
				return source.MoveNext();
			}

			public long Remove() {
				throw new NotSupportedException();
			}
		}

		#endregion
	}
}