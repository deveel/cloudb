using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbTable : IEnumerable<DbRow> {
		private readonly int id;
		private readonly DbTransaction transaction;
		private readonly DbTableSchema schema;
		private readonly DataFile propFile;
						
		private long version = 0;
		private long idSeq = -1;
		
		private DbRow newRow;
		
		private List<long> add_row_list = new List<long>();
		private List<long> delete_row_list = new List<long>();
		private bool schemaModified = false;
		
		internal DbTable(DbTransaction transaction, DataFile propFile, int id) {
			if (id < 1)
				throw new ApplicationException("id out of range.");
			
			this.transaction = transaction;
			this.id = id;
			this.propFile = propFile;
			
			schema = new DbTableSchema(this);
		}
		
		private Key RowIndexKey {
			get { return new Key(1, id, 1); }
		}
				
		private Key AddLogKey {
			get {return new Key(1, id, 3); }
		}
		
		private Key RemoveLogKey {
			get { return new Key(1, id, 4); }
		}
				
		internal long CurrentVersion {
			get { return version; }
		}
		
		internal SortedIndex DeletedIndex {
			get { return new SortedIndex(GetFile(RemoveLogKey)); }
		}
		
		internal SortedIndex AddedIndex {
			get { return new SortedIndex(GetFile(AddLogKey)); }
		}
		
		internal Properties TableProperties {
			get { return new Properties(propFile); }
		}
		
		public DbTableSchema Schema {
			get { return schema; }
		}
		
		public long RowCount {
			get {
				// Get the main index file
				// Get the index,
				SortedIndex index = new SortedIndex(GetFile(RowIndexKey));
				// Return the row count,
				return index.Count;
			}
		}

		public bool IsModified {
			get { return version > 0; }
		}
		
		public bool IsSchemaModified {
			get { return schemaModified; }
		}
		
		private void AddRowToIndexSet(long rowid) {
			// Get the set of columns that are indexed in this table,
			string[] indexedCols = schema.IndexedColumns;
			foreach (string col in indexedCols) {
				// Resolve the column name to an id, turn it into a sorted index,
				// and insert the row in the correct location in the index,
				long columnid = schema.GetColumnId(col);
				SortedIndex index = new SortedIndex(GetFile(GetIndexIdKey(columnid)));
				IIndexedObjectComparer<string> c = schema.GetIndexComparerForColumn(col, columnid);
				index.Insert(GetCellValue(rowid, columnid), rowid, c);
			}
		}
		
		private void RemoveRowFromIndexSet(long rowid) {
			// Get the set of columns that are indexed in this table,
			string[] indexedCols = schema.IndexedColumns;
			// For each index,
			foreach (string col in indexedCols) {
				// Resolve the column name to an id, turn it into a sorted index,
				// and insert the row in the correct location in the index,
				long columnid = schema.GetColumnId(col);
				SortedIndex index = new SortedIndex(GetFile(GetIndexIdKey(columnid)));
				IIndexedObjectComparer<string> c = schema.GetIndexComparerForColumn(col, columnid);
				index.Remove(GetCellValue(rowid, columnid), rowid, c);
			}
		}
		
		private void AddRowToRowSet(long rowid) {
			// Get the index object
			DataFile df = GetFile(RowIndexKey);
			SortedIndex row_set = new SortedIndex(df);
			// Add the row in rowid value sorted order
			row_set.InsertSortKey(rowid);
		}
		
		private void RemoveRowFromRowSet(long rowid) {
			// Get the index object
			DataFile df = GetFile(RowIndexKey);
			SortedIndex row_set = new SortedIndex(df);
			// Remove the row in rowid value sorted order
			row_set.RemoveSortKey(rowid);
		}

		
		internal long UniqueId() {
			if (idSeq == -1) {
				Properties p = TableProperties;
				long v = p.GetValue("k", 16);
				idSeq = v;
			}
			
			++idSeq;
			return idSeq - 1;
		}
				
		internal void OnTransactionEvent(string cmd, long arg) {
			if (cmd.Equals("InsertRow")) {
				add_row_list.Add(arg);
			} else if (cmd.Equals("DeleteRow")) {
				delete_row_list.Add(arg);
			} else {
				throw new ApplicationException("Unknown transaction command: " + cmd);
			}
			
			version++;
		}
		
		internal void OnTransactionEvent(string cmd, string arg) {
			schemaModified = true;
		}
		
		internal void PrepareCommit() {
			// Write the transaction log for this table,
			DataFile df = GetFile(AddLogKey);
			df.Delete();
			
			SortedIndex addlist = new SortedIndex(df);
			foreach (long v in add_row_list) {
				addlist.InsertSortKey(v);
			}
			
			df = GetFile(RemoveLogKey);
			df.Delete();
			
			SortedIndex deletelist = new SortedIndex(df);
			foreach (long v in delete_row_list) {
				if (addlist.ContainsSortKey(v)) {
					addlist.RemoveSortKey(v);
				} else {
					deletelist.InsertSortKey(v);
				}
			}
			
			// Set the id gen key
			if (idSeq != -1)
				TableProperties.SetValue("k", idSeq);
		}
		
		private Key GetRowIdKey(long rowid) {
			// Sanity check to prevent corruption of the table state
			if (rowid <= 12)
				throw new ApplicationException("rowid value out of bounds.");
			
			return new Key(1, id, rowid);
		}
		
		internal Key GetIndexIdKey(long columnid) {
			// Sanity check to prevent corruption of the table state
			if (columnid <= 12)
				throw new ApplicationException("rowid value out of bounds.");
			return new Key(1, id, columnid);
		}
				
		internal DataFile GetFile(Key k) {
			return transaction.Parent.GetFile(k, FileAccess.ReadWrite);
		}
		
		internal String GetCellValue(long rowid, long columnid) {
			Key row_key = GetRowIdKey(rowid);
			DataFile rowFile = GetFile(row_key);
			RowBuilder rowBuilder = new RowBuilder(rowFile);
			return rowBuilder.GetValue(columnid);
		}
		
		internal void BuildIndex(long columnid, IIndexedObjectComparer<string> index_collator) {
			// Get the index object
			DataFile df = GetFile(GetIndexIdKey(columnid));
			
			// Get the index and clear it,
			SortedIndex index = new SortedIndex(df);
			index.Clear();

			// For each row in this table,
			foreach(DbRow row in this) {
				// Get the column value and the rowid
				string columnValue = row.GetValue(columnid);
				long rowid = row.RowId;
				
				// Add it into the index,
				index.Insert(columnValue, rowid, index_collator);
			}
		}
		
		  internal void DeleteFully() {
			DataFile df;
			
			// Get the index list,
			string[] cols = schema.IndexedColumns;
			// Delete all the indexes,
			foreach (String col in cols) {
				long columnid = schema.GetColumnId(col);
				df = GetFile(GetIndexIdKey(columnid));
				df.Delete();
			}
			
			// Delete all the rows in reverse,
			foreach (DbRow row in this) {
				long rowid = row.RowId;
				df = GetFile(GetRowIdKey(rowid));
				df.Delete();
			}
			
			// Delete the main index,
			df = GetFile(RowIndexKey);
			df.Delete();
			
			// Delete the transaction info,
			df = GetFile(AddLogKey);
			df.Delete();
			df = GetFile(RemoveLogKey);
			df.Delete();
		}

		
		public DbRow NewRow() {
			if (newRow != null)
				throw new ApplicationException("Another row has been created and not committed.");
			
			newRow = new DbRow(this, -1);
			return newRow;
		}
		
		public void Insert(DbRow row) {
			if (row.RowId != -1)
				throw new ArgumentException("The row is not new.", "row");
			if (!row.IsDirty)
				throw new ArgumentException("The row has no values set.", "row");
			
			// Create a new rowid
			long rowid = UniqueId();
			DataFile df = GetFile(GetRowIdKey(rowid));
			
			// Build the row,
			RowBuilder builder = new RowBuilder(df);
			string[] cols = schema.Columns;
			foreach (string col in cols) {
				string colValue = row.GetValue(col);
				builder.SetValue(schema.GetColumnId(col), row[col]);
			}
			
			// Update the indexes
			AddRowToRowSet(rowid);
			AddRowToIndexSet(rowid);
			
			// Add this event to the transaction log,
			OnTransactionEvent("InsertRow", rowid);
			++version;
			
			row.IsDirty = false;
			row.RowId = rowid;
			
			newRow = null;
		}
		
		public void Update(DbRow row) {
			if (row.RowId == -1)
				throw new ArgumentException("The row is invalid.", "row");
			if (!row.IsDirty)
				throw new ArgumentException("The row has no values set.", "row");
			
			if (newRow != null)
				throw new ApplicationException("A new row is already opened.");
			
			// Check row is currently indexed,
			SortedIndex rowIndex = new SortedIndex(GetFile(RowIndexKey));
			if (!rowIndex.ContainsSortKey(row.RowId))
				throw new ApplicationException("Row being updated is not in the table");
			
			// Create a new rowid
			long rowid = UniqueId();
			
			RowBuilder builder = new RowBuilder(GetFile(GetRowIdKey(rowid)));
			string[] cols = schema.Columns;
			foreach (string col in cols) {
				string colValue = row.GetValue(col);
				builder.SetValue(schema.GetColumnId(col), row[col]);
			}
			
			// Update the indexes
			RemoveRowFromRowSet(row.RowId);
			RemoveRowFromIndexSet(row.RowId);
			AddRowToRowSet(rowid);
			AddRowToIndexSet(rowid);
			
			// Add this event to the transaction log,
			OnTransactionEvent("DeleteRow", row.RowId);
			OnTransactionEvent("InsertRow", rowid);
			DataFile row_df = GetFile(GetRowIdKey(row.RowId));
			row_df.Delete();
			++version;
		}
		
		 public DbRowCursor GetCursor() {
			SortedIndex list = new SortedIndex(GetFile(RowIndexKey));
			return new DbRowCursor(this, version, list.GetCursor());
		}
		
		IEnumerator<DbRow> IEnumerable<DbRow>.GetEnumerator() {
			return GetCursor();
		}
		
		IEnumerator IEnumerable.GetEnumerator() {
			return GetCursor();
		}
		
		public DbIndex GetIndex(string columnName) {
			schema.CheckColumnName(columnName);
			Properties p = TableProperties;
			long columnId = p.GetValue(columnName + ".id", -1);
			if (columnId == -1)
				throw new ApplicationException("Column '" + columnName + "' not found");
			
			bool indexed = p.GetValue(columnName + ".index", false);
			if (!indexed)
				throw new ApplicationException("Column '" + columnName + "' is not indexed");
			
			// Fetch the index object,
			SortedIndex list = new SortedIndex(GetFile(GetIndexIdKey(columnId)));
			
			// And return it,
			IIndexedObjectComparer<string> comparer = schema.GetIndexComparerForColumn(columnName, columnId);
			return new DbIndex(this, version, comparer, columnId, list);
		}
		
		#region RowBuilder
		
		private sealed class RowBuilder {
			private readonly DataFile file;
			
			public RowBuilder(DataFile file) {
				this.file = file;
			}
			
			public String GetValue(long columnid) {
				file.Position = 0;
				BinaryReader din = new BinaryReader(new DataFileStream(file));
				
				try {
					// If no size, return null
					int size = Math.Min((int) file.Length, Int32.MaxValue);
					if (size == 0)
						return null;
					
					// The number of columns stored in this row,
					int hsize = din.ReadInt32();
					
					for (int i = 0; i < hsize; ++i) {
						long sid = din.ReadInt64();
						int coffset = din.ReadInt32();
						
						if (sid == columnid) {
							file.Position = (4 + (hsize * 12) + coffset);
							
							din = new BinaryReader(new DataFileStream(file, FileAccess.Read));
							byte t = din.ReadByte();
							// Types (currently only supports string types (UTF8 encoded)).
							if (t != 1)
								throw new ApplicationException("Unknown cell type: " + t);
							
							// Read the UTF value and return
							return din.ReadString();
						}
					}
					
					// Otherwise not found, return null.
					return null;
				} catch (IOException e) {
					// Wrap IOException around a runtime exception
					throw new ApplicationException(e.Message, e);
				}
			}
			
			public void SetValue(long columnid, string value) {
				file.Position = 0;
				BinaryReader din = new BinaryReader(new DataFileStream(file, FileAccess.Read));
				
				try {
					int size = Math.Min((int) file.Length, Int32.MaxValue);
					
					// If file is not empty,
					if (size != 0) {
						// Check if the columnid already set,
						int hsize = din.ReadInt32();

						for (int i = 0; i < hsize; ++i) {
							long sid = din.ReadInt64();
							int coffset = din.ReadInt32();
							if (sid == columnid)
								throw new ApplicationException("Column value already set.");
						}
					}
					
					// Ok to add column,
					file.Position = 0;
					if (size == 0) {
						file.Write(1);
						file.Write(columnid);
						file.Write(0);
					} else {
						int count = file.ReadInt32();
						++count;
						file.Position = 0;
						file.Write(count);
						file.Shift(12);
						file.Write(columnid);
						file.Write((int) (file.Length - (count * 12) - 4));
						file.Position = file.Length;
					}
					
					// Write the string
					BinaryWriter dout = new BinaryWriter(new DataFileStream(file, FileAccess.Write));
					dout.Write((byte)1);
					dout.Write(value);
					dout.Flush();
					dout.Close();
				} catch (IOException e) {
					// Wrap IOException around a runtime exception
					throw new ApplicationException(e.Message, e);
				}
			}
		}
		
		#endregion
	}
}