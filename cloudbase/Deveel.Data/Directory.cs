using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data {
	internal class Directory {
		private readonly ITransaction transaction;
		private readonly Key metaKey;
		private readonly Key propertiesKey;
		private readonly Key indexKey;

		private readonly short itemKeyType;
		private readonly int itemKeyPrimary;

		private readonly IIndexedObjectComparer<string> comparer;

		private int version;

		public Directory(ITransaction transaction, Key metaKey, Key propertiesKey, Key indexKey, short itemKeyType, int itemKeyPrimary) {
			this.transaction = transaction;
			this.itemKeyPrimary = itemKeyPrimary;
			this.itemKeyType = itemKeyType;
			this.indexKey = indexKey;
			this.propertiesKey = propertiesKey;
			this.metaKey = metaKey;

			comparer = new ItemComparer(this);
		}

		private DataFile GetFile(Key key) {
			return transaction.GetFile(key, FileAccess.ReadWrite);
		}

		private Key GetKey(long id) {
			return new Key(itemKeyType, itemKeyPrimary, id);
		}

		private long UniqueId() {
			DataFile df = GetFile(metaKey);
			StringDictionary pset = new StringDictionary(df);
			long v = pset.GetValue("v", 16);
			pset.SetValue("v", v + 1);
			return v;
		}

		private string GetFileName(long id) {
			Key k = GetKey(id);
			DataFile df = GetFile(k);
			try {
				BinaryReader din = new BinaryReader(new DataFileStream(df), Encoding.Unicode);
				return din.ReadString();
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		public Key GetFileKey(string name) {
			StringDictionary pset = new StringDictionary(GetFile(propertiesKey));
			long id = pset.GetValue(name, -1);
			return id == -1 ? null : GetKey(id);
		}

		public Key CreateFile(string fileName) {
			++version;

			StringDictionary pset = new StringDictionary(GetFile(propertiesKey));
			// Assert the item isn't already stored,
			if (pset.GetValue(fileName, -1L) != -1L)
				throw new ApplicationException("Item already exists: " + fileName);

			// Generate a unique identifier for the file name,
			long id = UniqueId();

			pset.SetValue(fileName, id);
			SortedIndex iset = new SortedIndex(GetFile(indexKey));
			iset.Insert(fileName, id, comparer);

			Key item_key = GetKey(id);
			DataFile df = GetFile(item_key);
			try {
				BinaryWriter dout = new BinaryWriter(new DataFileStream(df), Encoding.Unicode);
				dout.Write(fileName);
			} catch (IOException e) {
				throw new ApplicationException(e.Message);
			}

			return item_key;
		}

		public Key DeleteFile(string name) {
			++version;

			StringDictionary pset = new StringDictionary(GetFile(propertiesKey));
			long id = pset.GetValue(name, -1);
			// Assert the item is stored,
			if (id == -1)
				throw new ApplicationException("Item not found: " + name);

			pset.SetValue(name, null);
			SortedIndex iset = new SortedIndex(GetFile(indexKey));
			iset.Remove(name, id, comparer);

			// Delete the associated datafile
			Key k = GetKey(id);
			DataFile df = GetFile(k);
			df.Delete();

			return k;
		}
		
		public IList<String> ListFiles() {
			return new FileList(this, new SortedIndex(GetFile(indexKey)));
		}

		public long Count {
			get {
				SortedIndex iset = new SortedIndex(GetFile(indexKey));
				return iset.Count;
			}
		}
		
		public DataFile GetFile(string name) {
			StringDictionary pset = new StringDictionary(GetFile(propertiesKey));
			long id = pset.GetValue(name, -1);
			if (id == -1)
				throw new ApplicationException("File not found: " + name);
			
			Key k = GetKey(id);
			DataFile df = GetFile(k);
			
			// Find out how large the header is, without actually reading it. This is
			// an optimization to improve queries that want to only find the size of
			// the file without touching the data.
			
			int headerSize;
			try {
				MemoryStream stream = new MemoryStream(64);
				BinaryWriter reader = new BinaryWriter(stream, Encoding.Unicode);
				reader.Write(name);
				reader.Flush();
				headerSize = (int) stream.Length;
				reader.Close();
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
			
			df.Position = headerSize;
			return new SubDataFile(df, headerSize);
		}

		public void CopyTo(string name, Directory dest) {
			++dest.version;

			StringDictionary pset = new StringDictionary(GetFile(propertiesKey));
			long id = pset.GetValue(name, -1);
			// Assert the item is stored,
			if (id == -1)
				throw new ApplicationException("Item not found: " + name);

			// Get the source data file item,
			Key sourceKey = GetKey(id);
			DataFile sourceFile = GetFile(sourceKey);

			// Add the item to the destination directory set
			Key destKey = dest.CreateFile(name);
			if (destKey == null)
				throw new ApplicationException("Destination file '" + name + "' not found.");

			DataFile destFile = dest.GetFile(destKey);

			// Copy the data,
			sourceFile.ReplicateTo(destFile);
		}
		
		#region FileList
		
		private class FileList : IList<String> {
			private readonly Directory ds;
			private readonly SortedIndex list;
			private readonly long localDirVersion;

			internal FileList(Directory ds, SortedIndex list) {
				this.ds = ds;
				localDirVersion = ds.version;
				this.list = list;
			}

			private void VerifyModifications() {
				// If the directory set changed while this list in use, generate an
				// error.
				if (localDirVersion != ds.version)
					throw new InvalidOperationException("Directory changed while enumerating");
			}

			public void RemoveAt(int index) {
				throw new NotSupportedException();
			}

			public string this[int index] {
				get {
					VerifyModifications();
					long id = list[index];
					return ds.GetFileName(id);
				}
				set { throw new NotSupportedException(); }
			}

			public int IndexOf(string o) {
				// Since we know the list is sorted and there are no duplicate entries,
				// we can resolve this one quickly
				/*
				TODO: not the best solution, but ...
				int i = this.BinarySearch(o);
				*/
				int i = new List<string>(this).BinarySearch(o);
				return i < 0 ? -1 : i;
			}

			public void Insert(int index, string item) {
				throw new NotSupportedException();
			}

			public int Count {
				get { return (int)Math.Min(list.Count, Int32.MaxValue); }
			}

			#region Implementation of IEnumerable

			public IEnumerator<string> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			#endregion

			#region Implementation of ICollection<string>

			public void Add(string item) {
				throw new NotSupportedException();
			}

			public void Clear() {
				throw new NotSupportedException();
			}

			public bool Contains(string item) {
				return IndexOf(item) > 0;
			}

			public void CopyTo(string[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(string item) {
				throw new NotSupportedException();
			}

			public bool IsReadOnly {
				get { return true; }
			}

			#endregion

			#region Enumerator

			private class Enumerator : IEnumerator<string> {
				public Enumerator(FileList list) {
					this.list = list;
					index = -1;
					count = list.Count;
				}

				private readonly FileList list;
				private int index;
				private int count;

				#region Implementation of IDisposable

				public void Dispose() {
				}

				#endregion

				#region Implementation of IEnumerator

				public bool MoveNext() {
					list.VerifyModifications();
					return ++index < count;
				}

				public void Reset() {
					index = -1;
					count = list.Count;
				}

				public string Current {
					get {
						list.VerifyModifications();
						return list[index];
					}
				}

				object IEnumerator.Current {
					get { return Current; }
				}

				#endregion
			}

			#endregion
		}
		
		#endregion

		#region ItemComparer

		private class ItemComparer : IIndexedObjectComparer<string> {
			private readonly Directory dir;

			public ItemComparer(Directory dir) {
				this.dir = dir;
			}

			public int Compare(long reference, string value) {
				// Nulls are ordered at the beginning
				string v = dir.GetFileName(reference);
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

		[DbTrusted]
#if DEBUG
		public
#endif
 class SubDataFile : DataFile {
			private readonly DataFile file;
			private readonly long start;

			public SubDataFile(DataFile file, long start) {
				this.file = file;
				this.start = start;
			}

			public override long Length {
				get { return file.Length - start; }
			}

			public override long Position {
				get { return file.Position - start; }
				set { file.Position = value + start; }
			}

			public override int Read(byte[] buffer, int offset, int count) {
				return file.Read(buffer, offset, count);
			}

			public override void Write(byte[] buffer, int offset, int count) {
				file.Write(buffer, offset, count);
			}

			public override void SetLength(long value) {
				file.SetLength(value + start);
			}

			public override void Shift(long offset) {
				file.Shift(offset);
			}

			public override void Delete() {
				file.SetLength(start);
			}

			public override void CopyTo(DataFile destFile, long size) {
				file.CopyTo(destFile, size);
			}

			public override void ReplicateTo(DataFile target) {
				// This is a little complex. If 'target' is an instance of SubDataFile
				// we use the raw 'ReplicateTo' method on the data files and preserve
				// the header on the target by making a copy of it before the ReplicateTo
				// function.
				// Otherwise, we use a 'CopyTo' implementation.

				// If replicating to a SubDataFile
				if (target is SubDataFile) {
					// Preserve the header of the target
					SubDataFile target_file = (SubDataFile)target;
					long header_size = target_file.start;
					if (header_size <= 8192) {
						DataFile target_df = target_file.file;
						// Make a copy of the header in the target,
						int ihead_size = (int)header_size;
						byte[] header = new byte[ihead_size];
						target_df.Position = 0;
						target_df.Read(header, 0, ihead_size);

						// Replicate the bases
						file.ReplicateTo(target_df);

						// Now 'target_df' will be a replica of this, so we need to copy
						// the previous header back on the target.
						// Remove the replicated header on the target and copy the old one
						// back.
						target_df.Position = start;
						target_df.Shift(ihead_size - start);
						target_df.Position = 0;
						target_df.Write(header, 0, ihead_size);
						// Set position per spec
						target_df.Position = target_df.Length;
						// Done.
						return;
					}
				}

				// Fall back to a copy-to implementation
				target.Delete();
				target.Position = 0;
				file.Position = start;
				file.CopyTo(target, file.Length - start);
			}
		}
		
		#endregion
	}
}