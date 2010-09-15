using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public sealed class DbIndex : IEnumerable<DbRow> {
		private readonly DbTable table;
		private readonly long table_version;
		private readonly IIndexedObjectComparer<string> collator;
		private readonly long columnid;
		private readonly IIndex index;
		private readonly long start, end;
	
		private DbIndex(DbIndex copied) {
			this.table = copied.table;
			this.table_version = copied.table_version;
			this.collator = copied.collator;
			this.columnid = copied.columnid;
			this.index = copied.index;
			this.start = copied.start;
			this.end = copied.end;
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

  
		public IEnumerator<DbRow> GetEnumerator()
		{
			throw new NotImplementedException();
		}
		
		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}