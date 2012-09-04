using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data {
	internal class Directory {
		private readonly ITransaction transaction;

		private readonly Key directoryPropertiesKey;
		private readonly Key propertySetKey;
		private readonly Key indexSetKey;

		private readonly short itemKeyType;
		private readonly int itemKeyPrimary;

		private readonly IIndexedObjectComparer<string> collator;

		private long directoryVersion;

		internal Directory(ITransaction transaction, Key directoryPropertiesKey, Key propertySetKey, Key indexSetKey, short itemKeyType, int itemKeyPrimary) {
			this.transaction = transaction;
			this.directoryPropertiesKey = directoryPropertiesKey;
			this.indexSetKey = indexSetKey;
			this.propertySetKey = propertySetKey;
			this.itemKeyType = itemKeyType;
			this.itemKeyPrimary = itemKeyPrimary;

			collator = new ItemComparer(this);
		}

		public IList<string> Items {
			get { return new ItemList(this, new SortedIndex(GetDataFile(indexSetKey))); }
		}

		public long Count {
			get {
				SortedIndex iset = new SortedIndex(GetDataFile(indexSetKey));
				return iset.Count;
			}
		}

		private IDataFile GetDataFile(Key k) {
			return transaction.GetFile(k, FileAccess.ReadWrite);
		}

		private Key GetItemKey(long id) {
			return new Key(itemKeyType, itemKeyPrimary, id);
		}

		private long GenerateId() {
			IDataFile df = GetDataFile(directoryPropertiesKey);
			StringDictionary pset = new StringDictionary(df);
			long v = pset.GetValue<long>("v", 16);
			pset.SetValue("v", v + 1);
			return v;
		}

		public string GetItemName(long id) {
			Key k = GetItemKey(id);
			IDataFile df = GetDataFile(k);
			try {
				BinaryReader din = new BinaryReader(new DataFileStream(df), Encoding.Unicode);
				return din.ReadString();
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		public Key AddItem(string name) {
			++directoryVersion;

			StringDictionary pset = new StringDictionary(GetDataFile(propertySetKey));
			// Assert the item isn't already stored,
			if (pset.GetValue<long>(name, -1) != -1)
				throw new ApplicationException("Item already exists: " + name);

			// Generate a unique identifier for the name,
			long id = GenerateId();

			pset.SetValue(name, id);
			SortedIndex iset = new SortedIndex(GetDataFile(indexSetKey));
			iset.Insert(name, id, collator);

			Key itemKey = GetItemKey(id);

			IDataFile df = GetDataFile(itemKey);
			try {
				BinaryWriter dout = new BinaryWriter(new DataFileStream(df), Encoding.Unicode);
				dout.Write(name);
			} catch (IOException e) {
				throw new ApplicationException(e.Message);
			}

			return itemKey;
		}

		public Key GetItem(string name) {
			StringDictionary pset = new StringDictionary(GetDataFile(propertySetKey));
			long id = pset.GetValue<long>(name, -1);
			if (id == -1)
				return null;
			return GetItemKey(id);
		}

		public Key RemoveItem(String name) {
			++directoryVersion;

			StringDictionary pset = new StringDictionary(GetDataFile(propertySetKey));
			long id = pset.GetValue<long>(name, -1);
			// Assert the item is stored,
			if (id == -1)
				throw new ApplicationException("Item not found: " + name);

			pset.SetValue(name, null);
			SortedIndex iset = new SortedIndex(GetDataFile(indexSetKey));
			iset.Remove(name, id, collator);

			// Delete the associated datafile
			Key k = GetItemKey(id);
			IDataFile df = GetDataFile(k);
			df.Delete();

			return k;
		}

		public IDataFile GetItemDataFile(string name) {
			StringDictionary pset = new StringDictionary(GetDataFile(propertySetKey));
			long id = pset.GetValue<long>(name, -1);
			// Assert the item is stored,
			if (id == -1)
				throw new ApplicationException("Item not found: " + name);

			Key k = GetItemKey(id);
			IDataFile df = GetDataFile(k);

			// Find out how large the header is, without actually reading it. This is
			// an optimization to improve queries that want to only find the size of
			// the file without touching the data.
			int headerSize;
			try {
				MemoryStream bout = new MemoryStream(64);
				BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
				dout.Write(name);
				dout.Flush();
				dout.Close();
				headerSize = (int) bout.Length;
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}

			df.Position = headerSize;
			return new SubDataFile(df, headerSize);
		}

		public void CopyTo(String name, Directory destination) {
			++destination.directoryVersion;

			StringDictionary pset = new StringDictionary(GetDataFile(propertySetKey));
			long id = pset.GetValue<long>(name, -1);
			// Assert the item is stored,
			if (id == -1)
				throw new ApplicationException("Item not found: " + name);

			// Get the source data file item,
			Key sourceK = GetItemKey(id);
			IDataFile sourceDf = GetDataFile(sourceK);

			// Get the item from the destination. Throw an error if the item not
			// already found in the destination file set.
			Key destK = destination.GetItem(name);
			if (destK == null)
				throw new ApplicationException("Item not in destination: " + name);

			IDataFile destinationDf = destination.GetDataFile(destK);

			// Copy the data,
			sourceDf.ReplicateTo(destinationDf);
		}


		#region ItemComparer

		private class ItemComparer : IIndexedObjectComparer<string> {
			private readonly Directory directory;

			public ItemComparer(Directory directory) {
				this.directory = directory;
			}

			public int Compare(long reference, string value) {
				// Nulls are ordered at the beginning
				string v = directory.GetItemName(reference);
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

		#region SubDataFile

		class SubDataFile : IDataFile {
			private readonly IDataFile df;
			private readonly long start;

			public SubDataFile(IDataFile df, long start) {
				this.df = df;
				this.start = start;
			}

			public long Length {
				get { return df.Length - start; }
			}

			public long Position {
				get { return df.Position - start; }
				set {
					if (value < 0)
						throw new ArgumentOutOfRangeException();

					df.Position = start + value;
				}
			}

			public int Read(byte[] buffer, int offset, int count) {
				return df.Read(buffer, offset, count);
			}

			public void SetLength(long value) {
				if (value < 0)
					throw new ArgumentOutOfRangeException();

				df.SetLength(start + value);
			}

			public void Shift(long offset) {
				df.Shift(offset);
			}

			public void Delete() {
				df.SetLength(start);
			}

			public void Write(byte[] buffer, int offset, int count) {
				df.Write(buffer, offset, count);
			}

			public void CopyTo(IDataFile destFile, long size) {
				df.CopyTo(destFile, size);
			}

			public void CopyFrom(IDataFile sourceFile, long size) {
				throw new NotImplementedException();
			}

			public void ReplicateTo(IDataFile destFile) {
				// This is a little complex. If 'destFile' is an instance of SubDataFile
				// we use the raw 'ReplicateTo' method on the data files and preserve
				// the header on the target by making a copy of it before the replicateTo
				// function.
				// Otherwise, we use a 'CopyTo' implementation.

				// If replicating to a SubDataFile
				if (destFile is SubDataFile) {
					// Preserve the header of the target
					SubDataFile targetFile = (SubDataFile) destFile;
					long headerSize = targetFile.start;
					if (headerSize <= 8192) {
						IDataFile targetDf = targetFile.df;
						// Make a copy of the header in the target,
						int iheadSize = (int) headerSize;
						byte[] header = new byte[iheadSize];
						targetDf.Position = 0;
						targetDf.Read(header, 0, iheadSize);

						// Replicate the bases
						df.ReplicateTo(targetDf);
						// Now 'target_df' will be a replica of this, so we need to copy
						// the previous header back on the target.
						// Remove the replicated header on the target and copy the old one
						// back.
						targetDf.Position = start;
						targetDf.Shift(iheadSize - start);
						targetDf.Position = 0;
						targetDf.Write(header, 0, iheadSize);
						// Set position per spec
						targetDf.Position = targetDf.Length;
						// Done.
						return;
					}
				}

				// Fall back to a copy-to implementation
				destFile.Delete();
				destFile.Position = 0;
				df.Position = start;
				df.CopyTo(destFile, df.Length - start);
			}

			public void ReplicateFrom(IDataFile sourceFile) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region ItemCollection

		[Trusted]
		private class ItemList : IList<string> {
			private readonly Directory directory;
			private readonly SortedIndex sortedIndex;
			private readonly long localDirVersion;

			public ItemList(Directory directory, SortedIndex sortedIndex) {
				this.directory = directory;
				this.sortedIndex = sortedIndex;
				localDirVersion = directory.directoryVersion;
			}

			public IEnumerator<string> GetEnumerator() {
				return new Enumerator(this);
			}

			public void Add(string item) {
				throw new NotSupportedException();
			}

			public void Clear() {
				throw new NotSupportedException();
			}

			public bool Contains(string item) {
				return IndexOf(item) != 0;
			}

			public void CopyTo(string[] array, int arrayIndex) {
				Enumerator en = new Enumerator(this);
				int i = 0, toCopy = array.Length - arrayIndex;
				while (en.MoveNext() && i < toCopy) {
					array[arrayIndex] = en.Current;
					arrayIndex++;
				}
			}

			public bool Remove(string item) {
				throw new NotSupportedException();
			}

			public int Count {
				get { return (int)Math.Min(sortedIndex.Count, Int32.MaxValue); }
			}

			public bool IsReadOnly {
				get { return true; }
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public int IndexOf(string item) {
				// Since we know the list is sorted and there are no duplicate entries,
				// we can resolve this one quickly
				int i = CollectionsUtil.BinarySearch(this, item);
				return i < 0 ? -1 : i;
			}

			public void Insert(int index, string item) {
				throw new NotSupportedException();
			}

			public void RemoveAt(int index) {
				throw new NotSupportedException();
			}

			public string this[int index] {
				get {
					if (localDirVersion != directory.directoryVersion)
						throw new InvalidOperationException("Directory changed while iterator in use");

					long id = sortedIndex[index];
					return directory.GetItemName(id);
				}
				set { throw new NotSupportedException(); }
			}

			#region Enumerator

			class Enumerator : IEnumerator<string> {
				private readonly ItemList list;
				private int count;
				private int index;

				public Enumerator(ItemList list) {
					this.list = list;
					Reset();
				}

				private void CheckValid() {
					if (index >= count)
						throw new InvalidOperationException();
				}

				public void Dispose() {
				}

				public bool MoveNext() {
					if (index >= count)
						return false;

					return ++index < count;
				}

				public void Reset() {
					count = list.Count;
					index = -1;
				}

				public string Current {
					get {
						CheckValid();
						return list[index];
					}
				}

				object IEnumerator.Current {
					get { return Current; }
				}
			}

			#endregion
		}


		#endregion
	}
}