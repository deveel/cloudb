using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Text;

namespace Deveel.Data {
	[Trusted]
	public sealed class DbTable : IEnumerable<DbRow> {
		private readonly int tableId;
		private readonly DbTransaction transaction;
		private readonly IDataFile propertiesFile;
		private readonly Key rowIndexKey;


		private Dictionary<string, long> columnIdMap;

		private String[] cachedColumnList;
		private String[] cachedIndexList;


		private Dictionary<String, String> rowBuffer;
		private long rowBufferId;
		private long currentVersion;
		private long currentIdGen = -1;

		private readonly List<long> addRowList = new List<long>();
		private readonly List<long> deleteRowList = new List<long>();

		private bool structuralModification;

		internal DbTable(DbTransaction transaction, IDataFile propertiesFile, int tableId) {
			if (tableId < 1)
				throw new ApplicationException("table_id out of range.");

			this.transaction = transaction;
			this.tableId = tableId;
			this.propertiesFile = propertiesFile;

			// Setup various key objects,

			rowIndexKey = new Key(1, tableId, 1);
		}

		internal Key PropertiesLog {
			get { return new Key(1, tableId, 2); }
		}

		internal Key AddLog {
			get { return new Key(1, tableId, 3); }
		}

		internal Key RemoveLog {
			get { return new Key(1, tableId, 4); }
		}

		internal bool Modified {
			get { return (currentVersion > 0); }
		}

		internal bool HasStructuralChanges {
			get { return structuralModification; }
		}

		internal SortedIndex Deletes {
			get { return new SortedIndex(GetDataFile(RemoveLog)); }
		}

		internal SortedIndex Adds {
			get { return new SortedIndex(GetDataFile(AddLog)); }
		}

		internal long CurrentVersion {
			get { return currentVersion; }
		}

		public long RowCount {
			get {
				// Get the main index file
				IDataFile df = GetDataFile(rowIndexKey);
				// Get the index,
				SortedIndex index = new SortedIndex(df);
				// Return the row count,
				return index.Count;
			}
		}

		public long ColumnCount {
			get { return ColumnNames.Length; }
		}

		public string[] ColumnNames {
			get {
				if (cachedColumnList == null) {
					// Return the column list
					StringDictionary p = TableProperties;
					string columnList = p.GetValue("column_list", "");
					string[] colArr = !columnList.Equals("") ? columnList.Split(',') : new string[0];
					cachedColumnList = colArr;
				}
				return (string[]) cachedColumnList.Clone();
			}
		}

		public string[] IndexedColumns {
			get {
				if (cachedIndexList == null) {
					// Return the column list
					StringDictionary p = TableProperties;
					string columnList = p.GetValue("index_column_list", "");
					string[] colArr = !columnList.Equals("") ? columnList.Split(',') : new String[0];
					cachedIndexList = colArr;
				}
				return (string[]) cachedIndexList.Clone();
			}
		}

		private StringDictionary TableProperties {
			get { return new StringDictionary(propertiesFile); }
		}

		private void AddTransactionEvent(string cmd, string arg) {
			structuralModification = true;
		}

		private void AddTransactionEvent(string cmd, long arg) {
			if (cmd.Equals("insertRow")) {
				addRowList.Add(arg);
			} else if (cmd.Equals("deleteRow")) {
				deleteRowList.Add(arg);
			} else {
				throw new ApplicationException("Unknown transaction command: " + cmd);
			}
		}

		private long GenerateId() {
			if (currentIdGen == -1) {
				StringDictionary p = TableProperties;
				long v = p.GetValue("k", 16);
				currentIdGen = v;
			}
			++currentIdGen;
			return currentIdGen - 1;
		}

		private static void CopyFile(IDataFile s, IDataFile d) {
			//    d.Delete();
			//    s.Position = 0;
			//    d.Position = 0;
			//    s.CopyTo(d, s.Length);
			s.ReplicateTo(d);
		}

		private Key GetRowIdKey(long rowid) {
			// Sanity check to prevent corruption of the table state
			if (rowid <= 12)
				throw new ApplicationException("rowid value out of bounds.");

			return new Key(1, tableId, rowid);
		}

		private Key GetIndexIdKey(long columnid) {
			// Sanity check to prevent corruption of the table state
			if (columnid <= 12)
				throw new ApplicationException("rowid value out of bounds.");

			return new Key(1, tableId, columnid);
		}

		private IDataFile GetDataFile(Key k) {
			return transaction.Transaction.GetFile(k, FileAccess.ReadWrite);
		}

		private void CheckColumnNameValid(string columnName) {
			if (columnName.Length <= 0 || columnName.Length > 1024) {
				throw new ApplicationException("Invalid column name size");
			}
			int sz = columnName.Length;
			for (int i = 0; i < sz; ++i) {
				char c = columnName[i];
				if (c == '.' || c == ',' || Char.IsWhiteSpace(c))
					throw new ApplicationException("Invalid character in column name");
			}
		}

		private IIndexedObjectComparer<string> GetIndexComparerFor(string columnName, long columnid) {
			StringDictionary p = TableProperties;
			string culture = p.GetValue(columnName + ".culture", null);
			return culture == null
				       ? (IIndexedObjectComparer<string>) new DbLexiStringComparer(this, columnid)
				       : new DbLocaleStringComparer(this, columnid, new CultureInfo(culture));
		}

		private void BuildIndex(long columnid, IIndexedObjectComparer<string> indexComparer) {
			// Get the index object
			IDataFile df = GetDataFile(GetIndexIdKey(columnid));
			// Get the index and clear it,
			SortedIndex index = new SortedIndex(df);
			index.Clear();

			// For each row in this table,
			foreach (DbRow row in this) {
				// Get the column value and the rowid
				string columnValue = row.GetValue(columnid);
				long rowid = row.RowId;
				// Add it into the index,
				index.Insert(columnValue, rowid, indexComparer);
			}
			// Done.
		}

		private void AddRowToIndexSet(long rowid) {
			// Get the set of columns that are indexed in this table,
			string[] indexedCols = IndexedColumns;
			foreach (String col in indexedCols) {
				// Resolve the column name to an id, turn it into a OrderedList64Bit,
				// and insert the row in the correct location in the index,
				long columnid = GetColumnId(col);
				IDataFile df = GetDataFile(GetIndexIdKey(columnid));
				SortedIndex index = new SortedIndex(df);
				IIndexedObjectComparer<string> indexComparer = GetIndexComparerFor(col, columnid);
				index.Insert(GetValue(rowid, columnid), rowid, indexComparer);
			}
		}

		private void RemoveRowFromIndexSet(long rowid) {
			// Get the set of columns that are indexed in this table,
			string[] indexedCols = IndexedColumns;
			// For each index,
			foreach (String col in indexedCols) {
				// Resolve the column name to an id, turn it into a OrderedList64Bit,
				// and insert the row in the correct location in the index,
				long columnid = GetColumnId(col);
				IDataFile df = GetDataFile(GetIndexIdKey(columnid));
				SortedIndex index = new SortedIndex(df);
				IIndexedObjectComparer<string> indexComparer = GetIndexComparerFor(col, columnid);
				index.Remove(GetValue(rowid, columnid), rowid, indexComparer);
			}
		}

		private void AddRowToRowSet(long rowid) {
			// Get the index object
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex rows = new SortedIndex(df);
			// Add the row in rowid value sorted order
			rows.InsertSortKey(rowid);
		}

		private void RemoveRowFromRowSet(long rowid) {
			// Get the index object
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex rows = new SortedIndex(df);
			// Remove the row in rowid value sorted order
			rows.RemoveSortKey(rowid);
		}

		internal long GetColumnId(String columnName) {
			// Maps a column name to the id assigned it. This method is backed by a
			// local cache to improve frequent lookup operations.
			CheckColumnNameValid(columnName);
			if (columnIdMap == null)
				columnIdMap = new Dictionary<string, long>();

			long columnId;
			if (!columnIdMap.TryGetValue(columnName, out columnId)) {
				StringDictionary p = TableProperties;
				columnId = p.GetValue(columnName + ".id", -1);
				if (columnId == -1)
					throw new ApplicationException("Column '" + columnName + "' not found");

				columnIdMap[columnName] = columnId;
			}
			return columnId;
		}

		internal void PrepareForCommit() {
			// Write the transaction log for this table,
			IDataFile df = GetDataFile(AddLog);
			df.Delete();
			SortedIndex addlist = new SortedIndex(df);
			foreach (long v in addRowList) {
				addlist.InsertSortKey(v);
			}

			df = GetDataFile(RemoveLog);
			df.Delete();
			SortedIndex deletelist = new SortedIndex(df);
			foreach (long v in deleteRowList) {
				if (addlist.ContainsSortKey(v)) {
					addlist.RemoveSortKey(v);
				} else {
					deletelist.InsertSortKey(v);
				}
			}

			// Set the id gen key
			if (currentIdGen != -1) {
				StringDictionary p = TableProperties;
				p.SetValue("k", currentIdGen);
			}
		}

		internal void MergeFrom(DbTable from, bool structuralChange, bool historicDataChange) {
			// If structural_change is true, this can only happen if 'from' is the
			// immediate child of this table.
			// If 'historic_data_change' is false, this can only happen if 'from' is
			// the immediate child of this table.

			// Handle structural change,
			if (structuralChange || historicDataChange == false) {
				// Fetch all the indexes,
				string[] fromIndexes = from.IndexedColumns;
				List<long> fromIndexIds = new List<long>(fromIndexes.Length);
				foreach (String findex in fromIndexes) {
					fromIndexIds.Add(from.GetColumnId(findex));
				}
				// Copy them into here,
				CopyFile(from.GetDataFile(from.rowIndexKey), GetDataFile(rowIndexKey));
				foreach (long indexId in fromIndexIds) {
					// Copy all the indexes here,
					CopyFile(from.GetDataFile(from.GetIndexIdKey(indexId)), GetDataFile(GetIndexIdKey(indexId)));
				}

				// Move the column and index information into this table,
				CopyFile(from.propertiesFile, propertiesFile);

				// Copy the transaction logs
				CopyFile(from.GetDataFile(from.AddLog), GetDataFile(AddLog));
				CopyFile(from.GetDataFile(from.RemoveLog), GetDataFile(RemoveLog));

				// Replay the add and remove transaction events
				SortedIndex addEvents = new SortedIndex(from.GetDataFile(from.AddLog));
				SortedIndex removeEvents = new SortedIndex(from.GetDataFile(from.RemoveLog));

				// Adds
				foreach (long rowid in addEvents) {
					CopyFile(from.GetDataFile(from.GetRowIdKey(rowid)), GetDataFile(GetRowIdKey(rowid)));
				}

				// Removes
				foreach (long rowid in removeEvents) {
					// Delete the row data file,
					GetDataFile(GetRowIdKey(rowid)).Delete();
				}
			} else {
				// If we are here, then we are merging a change that isn't a structural
				// change, and there are historical changes. Basically this means we
				// need to replay the add and remove events only, but more strictly,

				// Replay the add and remove transaction events
				SortedIndex addEvents = new SortedIndex(from.GetDataFile(from.AddLog));
				SortedIndex removeEvents = new SortedIndex(from.GetDataFile(from.RemoveLog));

				// Adds
				foreach (long fromRowid in addEvents) {
					// Generate a new id for the row,
					long toRowid = GenerateId();
					// Copy record to the new id in this table,
					CopyFile(from.GetDataFile(from.GetRowIdKey(fromRowid)), GetDataFile(GetRowIdKey(toRowid)));
					// Update indexes,
					AddRowToRowSet(toRowid);
					AddRowToIndexSet(toRowid);
					// Add this event to the transaction log,
					AddTransactionEvent("insertRow", toRowid);
				}

				// Removes
				foreach (long fromRowid in removeEvents) {
					// Update indexes,
					RemoveRowFromRowSet(fromRowid);
					RemoveRowFromIndexSet(fromRowid);

					// Delete the row data file,
					GetDataFile(GetRowIdKey(fromRowid)).Delete();

					// Add this event to the transaction log,
					AddTransactionEvent("deleteRow", fromRowid);
				}

				// Write out the transaction logs,
				PrepareForCommit();
			}

			// Invalidate all the cached info,
			columnIdMap = null;
			cachedColumnList = null;
			cachedIndexList = null;

			++currentVersion;
		}

		internal string GetValue(long rowid, long columnid) {
			Key rowKey = GetRowIdKey(rowid);
			IDataFile rowFile = GetDataFile(rowKey);
			RowBuilder rowBuilder = new RowBuilder(rowFile);
			return rowBuilder.GetValue(columnid);
		}

		internal void PreFetchValue(long rowid, long columnid) {
			// Create the key object,
			Key rowKey = GetRowIdKey(rowid);
			// Pass the hint on to the backed transaction,
			transaction.Transaction.PreFetchKeys(new Key[] {rowKey});
		}

		internal static string AddToColumnSet(string name, string set) {
			if (set == null || set.Equals(""))
				return name;

			List<string> cols = new List<string>(set.Split(','));
			cols.Add(name);
			return String.Join(",", cols.ToArray());
		}

		internal static string RemoveFromColumnSet(string name, string set) {
			if (set != null && !set.Equals("")) {
				List<string> cols = new List<string>(set.Split(','));
				if (cols.Remove(name))
					return String.Join(",", cols.ToArray());
			}

			throw new ApplicationException("Column '" + name + "' not found");
		}

		internal void DeleteFully() {
			// Get the range of keys stored for this table, and delete it.
			IDataRange dataRange = transaction.Transaction.GetRange(new Key(1, tableId, 0),
			                                                        new Key(1, tableId, Int64.MaxValue));
			dataRange.Delete();
		}

		public bool IsColumnIndexed(string columnName) {
			// TODO: CACHE this?
			CheckColumnNameValid(columnName);
			StringDictionary p = TableProperties;
			return p.GetValue(columnName + ".index", false);
		}

		public void AddColumn(string columnName) {
			CheckColumnNameValid(columnName);

			// Generate a column id
			long columnid = GenerateId();
			StringDictionary p = TableProperties;
			// Add to the column list,
			string columnList = p.GetValue("column_list", "");
			columnList = AddToColumnSet(columnName, columnList);
			p.SetValue("column_list", columnList);
			// Set a column name to columnid map,
			p.SetValue(columnName + ".id", columnid);

			cachedColumnList = null;
			// Add this event to the transaction log,
			AddTransactionEvent("addColumn", columnName);
			++currentVersion;
		}

		public void RemoveColumn(string column_name) {
			CheckColumnNameValid(column_name);

			StringDictionary p = TableProperties;
			// Add to the column list,
			string columnList = p.GetValue("column_list", "");
			columnList = RemoveFromColumnSet(column_name, columnList);
			// Check if column is indexed, generate error if it is,
			if (p.GetValue(column_name + ".index", false))
				throw new ApplicationException("Can't remove column " + column_name + " because it has an index");

			// Otherwise update and remove the column
			p.SetValue("column_list", columnList);
			// Set a column name to columnid map,
			p.SetValue(column_name + ".id", null);
			// Remove from column_id cache,
			if (columnIdMap != null)
				columnIdMap.Remove(column_name);

			// TODO: Remove the column data?


			cachedColumnList = null;
			// Add this event to the transaction log,
			AddTransactionEvent("removeColumn", column_name);
			++currentVersion;
		}

		public void AddIndex(string columnName) {
			AddIndex(columnName, null);
		}

		public void AddIndex(string columnName, string culture) {
			CheckColumnNameValid(columnName);

			StringDictionary p = TableProperties;
			// Check the column name exists,
			long columnid = p.GetValue(columnName + ".id", -1);
			if (columnid == -1)
				throw new ApplicationException("Column " + columnName + " not found");

			// Check if index property set,
			if (p.GetValue(columnName + ".index", false))
				throw new ApplicationException("Index already on column " + columnName);

			// Check the collator encoded string,
			if (culture != null)
				new CultureInfo(culture);

			// Add to the column list,
			string columnList = p.GetValue("index_column_list", "");
			columnList = AddToColumnSet(columnName, columnList);
			p.SetValue("index_column_list", columnList);
			// Set the index property,
			p.SetValue(columnName + ".index", true);
			if (culture != null)
				p.SetValue(columnName + ".culture", culture);

			// Build the index,
			IIndexedObjectComparer<string> indexComparer = GetIndexComparerFor(columnName, columnid);
			BuildIndex(columnid, indexComparer);

			cachedIndexList = null;
			// Add this event to the transaction log,
			AddTransactionEvent("addIndex", columnName);
			++currentVersion;
		}

		public void RemoveIndex(string columnName) {
			CheckColumnNameValid(columnName);

			StringDictionary p = TableProperties;
			// Check the column name index property,
			if (!p.GetValue(columnName + ".index", false))
				throw new ApplicationException("Column " + columnName + " not indexed");

			long columnid = p.GetValue(columnName + ".id", -1);
			if (columnid == -1)
				// For this error to occur here would indicate some sort of data model
				// corruption.
				throw new ApplicationException("Column " + columnName + " not found");

			// Remove from the index column list
			string columnList = p.GetValue("index_column_list", "");
			columnList = RemoveFromColumnSet(columnName, columnList);
			p.SetValue("index_column_list", columnList);
			// Remove the index property,
			p.SetValue(columnName + ".index", null);
			p.SetValue(columnName + ".collator", null);
			// Delete the index file,
			IDataFile indexFile = GetDataFile(GetIndexIdKey(columnid));
			indexFile.Delete();

			cachedIndexList = null;
			// Add this event to the transaction log,
			AddTransactionEvent("removeIndex", columnName);
			++currentVersion;
		}

		public DbRowCursor GetCursor() {
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex list = new SortedIndex(df);
			return new DbRowCursor(this, currentVersion, list.GetCursor());
		}

		public DbRowCursor GetReverseCursor() {
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex list = new SortedIndex(df);
			return new DbRowCursor(this, currentVersion, new DbIndex.ReverseCursor(list.GetCursor()));
		}

		IEnumerator<DbRow> IEnumerable<DbRow>.GetEnumerator() {
			return GetCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetCursor();
		}

		public void CopyRowsTo(ICollection<DbRow> rows) {
			foreach (DbRow row in this) {
				rows.Add(row);
			}
		}

		public DbIndex GetIndex(string columnName) {
			CheckColumnNameValid(columnName);

			StringDictionary p = TableProperties;
			long columnId = p.GetValue(columnName + ".id", -1);
			if (columnId == -1)
				throw new ApplicationException("Column '" + columnName + "' not found");

			if (!p.GetValue(columnName + ".index", false))
				throw new ApplicationException("Column '" + columnName + "' is not indexed");

			// Fetch the index object,
			IDataFile df = GetDataFile(GetIndexIdKey(columnId));
			SortedIndex list = new SortedIndex(df);

			// And return it,
			IIndexedObjectComparer<string> comparer = GetIndexComparerFor(columnName, columnId);
			return new DbIndex(this, currentVersion, comparer, columnId, list);
		}

		public void Delete(DbRow row) {
			long rowid = row.RowId;
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex rows = new SortedIndex(df);
			if (!rows.ContainsSortKey(rowid))
				throw new ApplicationException("Row being deleted is not in the table");

			// Remove the row from the main index,
			RemoveRowFromRowSet(rowid);
			// Remove the row from any indexes defined on the table,
			RemoveRowFromIndexSet(rowid);
			// Delete the row file
			IDataFile rowFile = GetDataFile(GetRowIdKey(rowid));
			rowFile.Delete();

			// Add this event to the transaction log,
			AddTransactionEvent("deleteRow", rowid);
			++currentVersion;
		}

		public int Delete(IEnumerable<DbRow> rows) {
			IEnumerator<DbRow> enumerator = rows.GetEnumerator();
			return Delete(enumerator);
		}

		public int Delete(IEnumerator<DbRow> rows) {
			int deleteCount = 0;
			while (rows.MoveNext()) {
				DbRow row = rows.Current;
				if (row == null)
					continue;

				long rowid = row.RowId;
				IDataFile df = GetDataFile(rowIndexKey);
				SortedIndex rowsIndex = new SortedIndex(df);
				if (rowsIndex.ContainsSortKey(rowid)) {
					// Remove the row from the main index,
					RemoveRowFromRowSet(rowid);
					// Remove the row from any indexes defined on the table,
					RemoveRowFromIndexSet(rowid);
					// Delete the row file
					IDataFile rowFile = GetDataFile(GetRowIdKey(rowid));
					rowFile.Delete();

					// Add this event to the transaction log,
					AddTransactionEvent("deleteRow", rowid);
					deleteCount++;
				}
			}

			++currentVersion;
			return deleteCount;			
		}

		public bool Empty() {
			ICollection<DbRow> rows = new Collection<DbRow>();
			CopyRowsTo(rows);
			long rowCount = RowCount;
			int deleteCount = Delete(rows);
			return rowCount == deleteCount;
		}

		public void BeginInsert() {
			if (rowBufferId != 0)
				throw new ApplicationException("State error: previous table operation not completed");

			if (rowBuffer == null)
				rowBuffer = new Dictionary<string, string>();

			rowBufferId = -1;
		}

		public DbRow NewRow() {
			return new DbRow(this, -1);
		}

		public void Insert(DbRow row) {
			BeginInsert();
			string[] columnNames = ColumnNames;
			foreach (string columnName in columnNames) {
				SetValue(columnName, row.GetValue(columnName));
			}
		}

		public void Update(DbRow row) {
			if (rowBufferId != 0)
				throw new ApplicationException("State error: previous table operation not completed");

			// Check row is currently indexed,
			long rowid = row.RowId;
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex rows = new SortedIndex(df);
			if (!rows.ContainsSortKey(rowid))
				throw new ApplicationException("Row being updated is not in the table");

			if (rowBuffer == null)
				rowBuffer = new Dictionary<string, string>();

			rowBufferId = rowid;

			// Copy from the existing data in the row,
			string[] cols = ColumnNames;
			foreach (string col in cols) {
				string val = row.GetValue(col);
				if (val != null) {
					rowBuffer[col] = val;
				}
			}
		}

		public void BeginUpdate(long rowid) {
			if (rowBufferId != 0)
				throw new ApplicationException("State error: previous table operation not completed");

			// Check row is currently indexed,
			IDataFile df = GetDataFile(rowIndexKey);
			SortedIndex rows = new SortedIndex(df);
			if (!rows.ContainsSortKey(rowid))
				throw new ApplicationException("Row being updated is not in the table");

			if (rowBuffer == null)
				rowBuffer = new Dictionary<string, string>();

			rowBufferId = rowid;
		}

		public void SetValue(string column, string value) {
			if (rowBufferId == 0)
				throw new ApplicationException("State error: not in insert or update state");

			if (value != null) {
				rowBuffer[column] = value;
			} else {
				rowBuffer.Remove(column);
			}
		}

		public void Complete() {
			if (rowBufferId == 0)
				throw new ApplicationException("State error: not in insert or update state");

			// Create a new rowid
			long rowid = GenerateId();
			IDataFile df = GetDataFile(GetRowIdKey(rowid));

			// Build the row,
			RowBuilder builder = new RowBuilder(df);
			foreach (KeyValuePair<string, string> pair in rowBuffer)
				builder.SetValue(GetColumnId(pair.Key), pair.Value);

			// If the operation is insert or update,
			if (rowBufferId == -1) {
				// Insert,
				// Update the indexes
				AddRowToRowSet(rowid);
				AddRowToIndexSet(rowid);
				// Add this event to the transaction log,
				AddTransactionEvent("insertRow", rowid);
				++currentVersion;
			} else {
				// Update,
				// Update the indexes
				RemoveRowFromRowSet(rowBufferId);
				RemoveRowFromIndexSet(rowBufferId);
				AddRowToRowSet(rowid);
				AddRowToIndexSet(rowid);
				// Add this event to the transaction log,
				AddTransactionEvent("deleteRow", rowBufferId);
				AddTransactionEvent("insertRow", rowid);
				IDataFile rowFile = GetDataFile(GetRowIdKey(rowBufferId));
				rowFile.Delete();
				++currentVersion;
			}

			// Clear the row buffer, etc
			rowBufferId = 0;
			rowBuffer.Clear();
		}

		#region RowBuilder

		private sealed class RowBuilder {

			private readonly IDataFile file;

			public RowBuilder(IDataFile file) {
				this.file = file;
			}

			public string GetValue(long columnid) {
				file.Position = 0;
				BinaryReader reader = new BinaryReader(new DataFileStream(file));

				try {
					// If no size, return null
					int size = Math.Min((int) file.Length, Int32.MaxValue);
					if (size == 0)
						return null;

					// The number of columns stored in this row,
					int hsize = reader.ReadInt32();

					for (int i = 0; i < hsize; ++i) {
						long sid = reader.ReadInt64();
						int coffset = reader.ReadInt32();
						if (sid == columnid) {
							file.Position = 4 + (hsize*12) + coffset;

							reader = new BinaryReader(new DataFileStream(file), Encoding.Unicode);
							byte t = reader.ReadByte();
							// Types (currently only supports string types (UTF8 encoded)).
							if (t != 1)
								throw new ApplicationException("Unknown cell type: " + t);

							// Read the UTF value and return
							return reader.ReadString();
						}
					}
					// Otherwise not found, return null.
					return null;
				} catch (IOException e) {
					// Wrap IOException around a runtime exception
					throw new ApplicationException(e.Message, e);
				}
			}

			public void SetValue(long columnid, String value) {
				file.Position = 0;
				BinaryReader din = new BinaryReader(new DataFileStream(file));

				try {
					int size = Math.Min((int) file.Length, Int32.MaxValue);
					// If file is not empty,
					if (size != 0) {
						// Check if the columnid already set,
						int hsize = din.ReadInt32();

						for (int i = 0; i < hsize; ++i) {
							long sid = din.ReadInt64();
							int coffset = din.ReadInt32();
							if (sid == columnid) {
								// Yes, so generate error,
								throw new ApplicationException("Column value already set.");
							}
						}
					}

					BinaryWriter writer = new BinaryWriter(new DataFileStream(file), Encoding.Unicode);

					// Ok to add column,
					file.Position = 0;
					if (size == 0) {
						writer.Write(1);
						writer.Write(columnid);
						writer.Write(0);
					} else {
						int count = din.ReadInt32();
						++count;
						file.Position = 0;
						writer.Write(count);
						file.Shift(12);
						writer.Write(columnid);
						writer.Write((int) (file.Length - (count*12) - 4));
						file.Position = file.Length;
					}

					// Write the string
					writer.Write((byte)1);
					writer.Write(value);
					writer.Flush();
					writer.Close();

				} catch (IOException e) {
					// Wrap IOException around a runtime exception
					throw new ApplicationException(e.Message, e);
				}
			}
		}

		#endregion

		#region DbLexiStringCollator

		private class DbLexiStringComparer : IIndexedObjectComparer<string> {
			private readonly DbTable table;
			private readonly long columnid;

			public DbLexiStringComparer(DbTable table, long columnid) {
				this.table = table;
				this.columnid = columnid;
			}

			public int Compare(long reference, string value) {
				// Nulls are ordered at the beginning
				string v = table.GetValue(reference, columnid);
				if (value == null && v == null)
					return 0;
				if (value == null)
					return 1;
				if (v == null)
					return -1;
				return String.Compare(v, value, StringComparison.Ordinal);
			}
		}

		#endregion

		#region DbLocaleStringComparer

		class DbLocaleStringComparer : IIndexedObjectComparer<string> {
			private readonly DbTable table;
			private readonly long columnid;
			private readonly CompareInfo compare;

			public DbLocaleStringComparer(DbTable table, long columnid, CultureInfo culture) {
				this.table = table;
				this.columnid = columnid;
				compare = culture.CompareInfo;
			}

			public int Compare(long reference, string value) {
				// Nulls are ordered at the beginning
				string v = table.GetValue(reference, columnid);
				if (value == null && v == null)
					return 0;
				if (value == null)
					return 1;
				if (v == null)
					return -1;

				return compare.Compare(v, value);
			}
		}

		#endregion
	}
}