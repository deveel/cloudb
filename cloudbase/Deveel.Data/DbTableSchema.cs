using System;
using System.Collections.Generic;
using System.Globalization;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbTableSchema {
		private readonly DbTable table;
		
		private string[] cachedColumns = null;
		private string[] cachedIndexes = null;
		
		private Dictionary<string, long> columnIdCache = null;

		
		internal DbTableSchema(DbTable table) {
			this.table = table;
		}
		
		internal void CheckColumnName(string columnName) {
			if (columnName.Length <= 0 || columnName.Length > 1024)
				throw new ApplicationException("Invalid column name size");
			
			int sz = columnName.Length;
			for (int i = 0; i < sz; ++i) {
				char c = columnName[i];
				if (c == '.' || c == ',' || Char.IsWhiteSpace(c))
					throw new ApplicationException("Invalid character in column name");
			}
		}
		
		private CultureInfo GetAndCheckLocale(string locale) {
			CultureInfo l;
			
			if (locale.Length == 2) {
				String lang = locale;
				l = new CultureInfo(lang);
			} else if (locale.Length == 4) {
				string lang = locale.Substring(0, 2);
				string country = locale.Substring(2);
				l = new CultureInfo(lang + "-" + country);
			} else {
				throw new ApplicationException("Invalid locale encoding");
			}
			
			// Do a test compare with the collator,
			CompareInfo c = l.CompareInfo;
			c.Compare("a", "b");
			
			return l;
		}
		
		private string AddToColumns(string name, string set) {
			if (set == null || set.Equals(""))
				return name;
		
			string[] cols = set.Split(',');
			string[] newCols = new string[cols.Length + 1];
			Array.Copy(cols, 0, newCols, 0, cols.Length);
			newCols[newCols.Length - 1] = name;
			return String.Join(",", newCols);
		}
		
		private string RemoveFromColumns(string name, string set) {
			if (set == null || set.Equals(""))
				throw new ApplicationException("Column '" + name + "' not found.");
			
			int removeIndex = -1;
			string[] cols = set.Split(',');
			for(int i = 0; i < cols.Length; i++) {
				string col = cols[i];
				if (col.Equals(name)) {
					removeIndex = i;
					break;
				}
			}
			
			if (removeIndex != -1) {
				string[] newCols = new string[cols.Length - 1];
				Array.Copy(cols, 0, newCols, 0, removeIndex - 1);
				Array.Copy(cols, removeIndex + 1, newCols, removeIndex, cols.Length - removeIndex);
				cols = newCols;
			}
			
			return String.Join(",", cols);
		}
		
		public string[] Columns {
			get {
				if (cachedColumns == null) {
					// Return the column list
					Properties p = table.TableProperties;
					string column_list = p.GetValue("columns", "");
					string[] columnArray;
					if (!column_list.Equals("")) {
						columnArray = column_list.Split(',');
					} else {
						columnArray = new string[0];
					}
					
					cachedColumns = columnArray;
				}
				
				return (string[]) cachedColumns.Clone();
			}
		}
		
		public long ColumnCount {
			get { return Columns.Length; }
		}
		
		public string[] IndexedColumns {
			get {
				if (cachedIndexes == null) {
					// Return the column list
					Properties p = table.TableProperties;
					string columns = p.GetValue("index_columns", "");
					string[] colArray;
					if (!columns.Equals("")) {
						colArray = columns.Split(',');
					} else {
						colArray = new string[0];
					}
					cachedIndexes = colArray;
				}
				
				return (string[]) cachedIndexes.Clone();
			}
		}
		
		internal long GetColumnId(string columnName) {
			// Maps a column name to the id assigned it. This method is backed by a
			// local cache to improve frequent lookup operations.
			CheckColumnName(columnName);
			
			if (columnIdCache == null)
				columnIdCache = new Dictionary<string, long>();
			
			long columnId;
			if (!columnIdCache.TryGetValue(columnName, out columnId)) {
				Properties p = table.TableProperties;
				columnId = p.GetValue(columnName + ".id", -1);
				if (columnId == -1)
					throw new ApplicationException("Column '" + columnName + "' not found");
		
				columnIdCache[columnName] = columnId;
			}
			
			return columnId;
		}

		
		public void AddColumn(string columnName) {
			CheckColumnName(columnName);
			
			// Generate a column id
			long columnid = table.UniqueId();
			Properties p = table.TableProperties;
			
			// Add to the column list,
			string columns = p.GetValue("columns", String.Empty);
			columns = AddToColumns(columnName, columns);
			p.SetValue("columns", columns);
			
			// Set a column name to columnid map,
			p.SetValue(columnName + ".id", columnid);
			
			cachedColumns = null;
			// Add this event to the transaction log,
			table.OnTransactionEvent("AddColumn", columnName);
		}
		
		  public void RemoveColumn(string columnName) {
			CheckColumnName(columnName);
			
			Properties p = table.TableProperties;
			
			// Add to the column list,
			string column_list = p.GetValue("columns", String.Empty);
			column_list = RemoveFromColumns(columnName, column_list);
			
			// Check if column is indexed, generate error if it is,
			bool indexed = p.GetValue(columnName + ".index", false);
			if (indexed)
				throw new ApplicationException("Cannot remove indexed column " + columnName + ".");
			
			// Otherwise update and remove the column
			p.SetValue("columns", column_list);
			
			// Set a column name to columnid map,
			p.SetValue(columnName + ".id", null);
			
			// Remove from column_id cache,
			if (columnIdCache != null)
				columnIdCache.Remove(columnName);

			//TODO: remove all data for the column ...
			
			cachedColumns = null;
			
			// Add this event to the transaction log,
			table.OnTransactionEvent("RemoveColumn", columnName);
		}
		
		public void AddIndex(string columnName, string locale) {
			CheckColumnName(columnName);
			
			Properties p = table.TableProperties;
			
			// Check the column name exists,
			long columnid = p.GetValue(columnName + ".id", -1);
			if (columnid == -1)
				throw new ApplicationException("Column " + columnName + " not found");
			
			// Check if index property set,
			if (p.GetValue(columnName + ".index", false))
				throw new ApplicationException("Index already on column " + columnName);
			
			// Check the collator encoded string,
			if (locale != null)
				GetAndCheckLocale(locale);
			
			// Add to the column list,
			string column_list = p.GetValue("index_columns", String.Empty);
			column_list = AddToColumns(columnName, column_list);
			p.SetValue("index_columns", column_list);
			
			// Set the index property,
			p.SetValue(columnName + ".index", true);
			if (locale != null)
				p.SetValue(columnName + ".collator", locale);
			
			// Build the index,
			IIndexedObjectComparer<string> indexComparer = GetIndexComparerForColumn(columnName, columnid);
			table.BuildIndex(columnid, indexComparer);
			
			cachedIndexes = null;
			
			// Add this event to the transaction log,
			table.OnTransactionEvent("AddIndex", columnName);
		}
		
		internal IIndexedObjectComparer<string> GetIndexComparerForColumn(string columnName, long columnid) {
			Properties p = table.TableProperties;
			string localeStr = p.GetValue(columnName + "collator", null);
			if (localeStr == null) {
				return new LexiStringComparer(table, columnid);
			} else {
				CultureInfo locale = GetAndCheckLocale(localeStr);
				return new LocaleStringComparer(table, columnid, locale);
			}
		}

		
		public void AddIndex(string column_name) {
			AddIndex(column_name, null);
		}
		
		public void RemoveIndex(string columnName) {
			CheckColumnName(columnName);
			
			Properties p = table.TableProperties;
			
			// Check the column name index property,
			bool is_indexed = p.GetValue(columnName + ".index", false);
			if (!is_indexed)
				throw new ApplicationException("Column " + columnName + " not indexed");
			
			long columnid = p.GetValue(columnName + ".id", -1);
			if (columnid == -1)
				// For this error to occur here would indicate some sort of data model
				// corruption.
				throw new ApplicationException("Column " + columnName + " not found");
			
			// Remove from the index column list
			string column_list = p.GetValue("index_columns", "");
			column_list = RemoveFromColumns(columnName, column_list);
			p.SetValue("index_columns", column_list);
			
			// Remove the index property,
			p.SetValue(columnName + ".index", null);
			p.SetValue(columnName + ".collator", null);
			
			// Delete the index file,
			DataFile index_file = table.GetFile(table.GetIndexIdKey(columnid));
			index_file.Delete();
			
			cachedIndexes = null;
			
			// Add this event to the transaction log,
			table.OnTransactionEvent("RemoveIndex", columnName);
		}
		
		  public bool IsColumnIndexed(string columnName) {
			CheckColumnName(columnName);
			Properties p = table.TableProperties;
			return p.GetValue(columnName + ".index", false);
		}
		
		#region LexiStringComparer
		
		class LexiStringComparer : IIndexedObjectComparer<string> {
			private readonly DbTable table;
			private readonly long columnid;
			
			public LexiStringComparer(DbTable table, long columnid) {
				this.columnid = columnid;
				this.table =table;
			}

			public int Compare(long reference, string value) {
				// Nulls are ordered at the beginning
				string v = table.GetCellValue(reference, columnid);
				if (value == null && v == null)
					return 0;
				if (value == null)
					return 1;
				if (v == null)
					return -1;
				return v.CompareTo(value);
			}
		}
		
		#endregion
		
		#region LocaleStringComparer
		
		class LocaleStringComparer : IIndexedObjectComparer<string> {
			private readonly DbTable table;
			private readonly long columnid;
			private readonly CompareInfo comparer;

			public LocaleStringComparer(DbTable table, long columnid, CultureInfo locale) {
				this.table = table;
				this.columnid = columnid;
				this.comparer = locale.CompareInfo;
			}
			
			public int Compare(long reference, string value) {
				// Nulls are ordered at the beginning
				string v = table.GetCellValue(reference, columnid);
				if (value == null && v == null)
					return 0;
				if (value == null)
					return 1;
				if (v == null)
					return -1;
				return comparer.Compare(v, value);
			}
		}
		
		#endregion
	}
}