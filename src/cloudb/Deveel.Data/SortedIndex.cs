﻿//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of  <see cref="IIndex"/> that is backed 
	/// by a <see cref="IDataFile" /> and which values are ordered.
	/// </summary>
	/// <remarks>
	/// When modifications happen, the change is immediately mapped 
	/// into the underlying data file, growing or shrinking the size 
	/// of the underlying data file according to the operation done
	/// on the index.
	/// <para>
	/// This implementation offers a practical way of representing a 
	/// sorted index of objects within a database. The collation of the 
	/// list items can be defined via an instance of the <see cref="IIndexedObjectComparer{T}"/>
	/// interface, or the order may be defined as a function of the index 
	/// value itself, as per the <see cref="IIndex"/>.
	/// </para>
	/// <para>
	/// This list object supports containing duplicate values.
	/// </para>
	/// </remarks>
	public class SortedIndex : IIndex {
		private readonly IDataFile file;
		private readonly bool readOnly;

		private readonly BinaryReader fileReader;
		private readonly BinaryWriter fileWriter;

		/// <summary>
		/// Constructs an instance of the index, wrapped around the given
		/// <see cref="IDataFile"/>.
		/// </summary>
		/// <param name="file">The underlying <see cref="IDataFile"/> where the
		/// data of the index will be reflected.</param>
		/// <param name="readOnly">Indicates whether any change to the index
		/// is it possible (if <b>false</b>) or if is it a read-only instance
		/// (if <b>true</b>).</param>
		public SortedIndex(IDataFile file, bool readOnly) {
			this.file = file;
			this.readOnly = readOnly;

			fileReader = new BinaryReader(new DataFileStream(file));
			fileWriter = new BinaryWriter(new DataFileStream(file));
		}

		/// <summary>
		/// Constructs an instance of the index, wrapped around the given
		/// <see cref="IDataFile"/>.
		/// </summary>
		/// <param name="file">The underlying <see cref="IDataFile"/> where the
		/// data of the index will be reflected.</param>
		/// <remarks>
		/// Instances of <see cref="SortedIndex"/> constructed with this
		/// constructor are always mutable.
		/// </remarks>
		public SortedIndex(IDataFile file)
			: this(file, false) {
		}

		public static readonly IIndexedObjectComparer<long> KeyComparer = new KeyComparerImpl();

		IEnumerator<long> IEnumerable<long>.GetEnumerator() {
			return GetCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetCursor();
		}

		public long Count {
			get { return file.Length / 8; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		private long SearchFirst<T>(T value, IIndexedObjectComparer<T> c, long low, long high) {
			if (low > high)
				return -1;

			while (true) {
				// If low is the same as high, we are either at the first value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					for (long i = low; i <= high; ++i) {
						file.Position = (i * 8);
						long val = fileReader.ReadInt64();
						int res = c.Compare(val, value);

						if (res == 0)
							return i;
						if (res > 0)
							return -(i + 1);
					}
					return -(high + 2);
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				file.Position =(mid * 8);
				long midVal = fileReader.ReadInt64();

				// Compare it with the value
				int res1 = c.Compare(midVal, value);
				if (res1 < 0) {
					low = mid + 1;
				} else if (res1 > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					high = mid;
				}
			}
		}

		private long SearchLast<T>(T value, IIndexedObjectComparer<T> c, long low, long high) {
			if (low > high)
				return -1;

			while (true) {
				// If low is the same as high, we are either at the last value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					for (long i = high; i >= low; --i) {
						file.Position =(i * 8);
						long val = fileReader.ReadInt64();
						int res = c.Compare(val, value);
						if (res == 0)
							return i;
						if (res < 0)
							return -(i + 2);
					}
					return -(low + 1);
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				file.Position =(mid * 8);
				long midVal = fileReader.ReadInt64();

				// Compare it with the value
				int res1 = c.Compare(midVal, value);
				if (res1 < 0) {
					low = mid + 1;
				} else if (res1 > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					low = mid;
				}
			}

		}

		private void SearchFirstAndLast<T>(T value, IIndexedObjectComparer<T> c, out long first, out long last) {
			long low = 0;
			long high = Count - 1;

			if (low > high) {
				first = -1;
				last = -1;
				return;
			}

			while (true) {
				// If low is the same as high, we are either at the first value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					first = SearchFirst(value, c, low, high);
					last = SearchLast(value, c, low, high);
					return;
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				file.Position = (mid * 8);
				long midVal = fileReader.ReadInt64();

				// Compare it with the value
				int res = c.Compare(midVal, value);
				if (res < 0) {
					low = mid + 1;
				} else if (res > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					first = SearchFirst(value, c, low, high);
					last = SearchLast(value, c, low, high);
					return;
				}
			}
		}

		private void CheckReadOnly() {
			if (readOnly)
				throw new ApplicationException("The source is read-only.");
		}

		public void Clear() {
			CheckReadOnly();
			file.SetLength(0);
		}

		public void Clear(long offset, long size) {
			CheckReadOnly();

			if (offset < 0 || offset + size > Count)
				throw new ArgumentOutOfRangeException();

			file.Position = (offset + size) * 8;
			file.Shift(-(size*8));
		}

		public long SearchFirst<T>(T value, IIndexedObjectComparer<T> c) {
			long low = 0;
			long high = Count - 1;

			return SearchFirst(value, c, low, high);
		}

		public long SearchLast<T>(T value, IIndexedObjectComparer<T> c) {
			long low = 0;
			long high = Count - 1;

			return SearchLast(value, c, low, high);
		}

		public long this[long offset] {
			get {
				file.Position = (offset * 8);
				return fileReader.ReadInt64();
			}
		}

		public IIndexCursor GetCursor(long start, long end) {
			// Make sure start and end aren't out of bounds
			if (start < 0 || end > Count || start - 1 > end)
				throw new ArgumentOutOfRangeException();

			return new Cursor(this, start, end);
		}

		public IIndexCursor GetCursor() {
			return GetCursor(0, Count - 1);
		}

		public void Insert<T>(T value, long index, IIndexedObjectComparer<T> c) {
			CheckReadOnly();

			// Search for the position of the last value in the set, 
			long pos = SearchLast(value, c);
			// If pos < 0 then the value was not found,
			if (pos < 0) {
				// Correct it to the point where the value must be inserted
				pos = -(pos + 1);
			} else {
				// If the value was found in the set, insert after the last value,
				++pos;
			}

			// Insert the value by moving to the position, shifting the data 8 bytes
			// and writing the long value.
			file.Position = (pos * 8);
			file.Shift(8);
			fileWriter.Write(index);
		}

		public bool InsertUnique<T>(T value, long index, IIndexedObjectComparer<T> c) {
			CheckReadOnly();

			// Search for the position of the last value in the set, 
			long pos = SearchLast(value, c);
			// If pos < 0 then the value was not found,
			if (pos < 0) {
				// Correct it to the point where the value must be inserted
				pos = -(pos + 1);
			} else {
				// If the value was found in the set, return false and don't change the
				// list.
				return false;
			}

			// Insert the value by moving to the position, shifting the data 8 bytes
			// and writing the long value.
			file.Position = (pos * 8);
			file.Shift(8);
			fileWriter.Write(index);
			// Return true because we changed the list,
			return true;
		}

		public void Remove<T>(T value, long index, IIndexedObjectComparer<T> c) {
			CheckReadOnly();

			// Search for the position of the last value in the set, 
			long p1, p2;
			SearchFirstAndLast(value, c, out p1, out p2);

			// If the value isn't found report the error,
			if (p1 < 0)
				throw new ApplicationException("Value '" + value + "' was not found in the set.");

			IIndexCursor cursor = GetCursor(p1, p2);
			while (cursor.MoveNext()) {
				if (cursor.Current == index) {
					// Remove the value and return
					cursor.Remove();
					return;
				}
			}
		}

		public void Add(long index) {
			CheckReadOnly();

			file.Position = file.Length;
			fileWriter.Write(index);
		}

		public void Insert(long index, long offset) {
			CheckReadOnly();

			if (offset < 0)
				throw new ArgumentOutOfRangeException("offset");

			long sz = Count;
			// Shift and insert
			if (offset < sz) {
				file.Position = (offset*8);
				file.Shift(8);
				fileWriter.Write(index);
			}
				// Insert at end
			else if (offset == sz) {
				file.Position = (sz*8);
				fileWriter.Write(index);
			}
		}

		public long RemoveAt(long offset) {
			CheckReadOnly();

			if (offset < 0 || offset > Count)
				throw new ArgumentOutOfRangeException("offset");

			file.Position = (offset*8);
			// Read the value then remove it
			long val = fileReader.ReadInt64();
			file.Shift(-8);
			// Return the value
			return val;
		}

		public void InsertSortKey(long index) {
			Insert(index, index, KeyComparer);
		}

		public void RemoveSortKey(long index) {
			Remove(index, index, KeyComparer);
		}

		public bool ContainsSortKey(long index) {
			return SearchFirst(index, KeyComparer) >= 0;
		}

		#region KeyComparerImpl

		private class KeyComparerImpl : IIndexedObjectComparer<long> {
			public int Compare(long offset, long value) {
				if (offset > value)
					return 1;
				if (offset < value)
					return -1;
				return 0;
			}
		}

		#endregion

		#region Cursor

		private class Cursor : IIndexCursor {
			private readonly SortedIndex index;
			private readonly long start;
			private long end;
			private long p = -1;
			private int lastDir;

			public Cursor(SortedIndex index, long start, long end) {
				this.index = index;
				this.end = end;
				this.start = start;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				if (++p + start <= end) {
					lastDir = 1;
					return true;
				}

				return false;
			}

			public void Reset() {
				p = -1;
				lastDir = 0;
				index.file.Position = start * 8;
			}

			public long Current {
				get {
					// Check the point is within the bounds of the iterator,
					if (p < 0 || start + p > end)
						throw new IndexOutOfRangeException();

					// Change the position and fetch the data,
					index.file.Position = ((start + p) * 8);
					return index.fileReader.ReadInt64();
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public object Clone() {
				return new Cursor(index, start, end);
			}

			public long Count {
				get { return (end - start) + 1; }
			}

			public long Position {
				get { return p; }
				set { 
					p = value;
					lastDir = 0;
				}
			}

			public bool MoveBack() {
				if (--p > 0) {
					lastDir = 2;
					return true;
				}

				return false;
			}

			public long Remove() {
				index.file.Position = ((start + p) * 8);
				long v = index.fileReader.ReadInt64();
				index.file.Shift(-8);

				if (lastDir == 1)
					--p;

				--end;

				// Returns the value we removed,
				return v;
			}
		}

		#endregion
	}
}