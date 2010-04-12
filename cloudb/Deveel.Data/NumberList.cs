using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class NumberList : ISortedList<long> {
		public NumberList(DataFile file, bool immutable) {
			this.file = file;
			this.immutable = immutable;
		}

		public NumberList(DataFile file)
			: this(file, false) {
		}

		private readonly DataFile file;
		private readonly IIndexedObjectComparer<long> comparer = new KeyComparer();
		private readonly bool immutable;

		private class KeyComparer : IIndexedObjectComparer<long> {
			public int Compare(long reference, long value) {
				if (reference > value)
					return 1;
				if (reference < value)
					return -1;
				return 0;
			}
		}

		private long SearchFirst(long value, IIndexedObjectComparer<long> c, long low, long high) {
			if (low > high)
				return -1;

			while (true) {
				// If low is the same as high, we are either at the first value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					for (long i = low; i <= high; ++i) {
						file.Position = i * 8;
						long val = file.ReadInt64();
						int res1 = c.Compare(val, value);
						if (res1 == 0)
							return i;
						if (res1 > 0)
							return -(i + 1);
					}
					return -(high + 2);
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				file.Position = mid * 8;
				long mid_val = file.ReadInt64();

				// Compare it with the value
				int res = c.Compare(mid_val, value);
				if (res < 0) {
					low = mid + 1;
				} else if (res > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					high = mid;
				}
			}
		}

		private long SearchLast(long value, IIndexedObjectComparer<long> c, long low, long high) {
			if (low > high)
				return -1;

			while (true) {
				// If low is the same as high, we are either at the last value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					for (long i = high; i >= low; --i) {
						file.Position = i * 8;
						long val = file.ReadInt64();
						int res1 = c.Compare(val, value);
						if (res1 == 0)
							return i;
						if (res1 < 0)
							return -(i + 2);
					}
					return -(low + 1);
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				file.Position = mid * 8;
				long mid_val = file.ReadInt64();

				// Compare it with the value
				int res = c.Compare(mid_val, value);
				if (res < 0) {
					low = mid + 1;
				} else if (res > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					low = mid;
				}
			}
		}

		private void SearchFirstAndLast(long value, IIndexedObjectComparer<long> c, long[] result) {
			long low = 0;
			long high = Count - 1;

			if (low > high) {
				result[0] = -1;
				result[1] = -1;
				return;
			}

			while (true) {
				// If low is the same as high, we are either at the first value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					result[0] = SearchFirst(value, c, low, high);
					result[1] = SearchLast(value, c, low, high);
					return;
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				file.Position = mid * 8;
				long mid_val = file.ReadInt64();

				// Compare it with the value
				int res = c.Compare(mid_val, value);
				if (res < 0) {
					low = mid + 1;
				} else if (res > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					result[0] = SearchFirst(value, c, low, high);
					result[1] = SearchLast(value, c, low, high);
					return;
				}
			}
		}

		#region Implementation of IEnumerable

		IEnumerator<long> IEnumerable<long>.GetEnumerator() {
			return GetEnumerator(0, Count - 1);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#endregion

		#region Implementation of ICollection<long>

		public void Add(long item) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

			file.Position = file.Length;
			file.Write(item);
		}

		public void Clear() {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

			file.SetLength(0);
		}

		public void Clear(long index, long size) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

			if (index >= 0 && index + size <= Count) {
				file.Position = (index + size) * 8;
				file.Shift(-(size * 8));
			} else {
				throw new ArgumentOutOfRangeException();
			}
		}

		bool ICollection<long>.Contains(long item) {
			return ContainsSort(item);
		}

		public void CopyTo(long[] array, int arrayIndex) {
			throw new NotImplementedException();
		}

		bool ICollection<long>.Remove(long item) {
			return RemoveSort(item);
		}

		int ICollection<long>.Count {
			get { return (int) Count; }
		}

		public long Count {
			get { return file.Length / 8; }
		}

		public bool IsReadOnly {
			get { return false; }
		}

		#endregion

		#region Implementation of IList<long>

		public int IndexOf(long item) {
			throw new NotImplementedException();
		}

		void IList<long>.Insert(int index, long item) {
			Insert(index, item);
		}

		void IList<long>.RemoveAt(int index) {
			RemoveAt(index);
		}

		long IList<long>.this[int index] {
			get { return this[index]; }
			set { throw new NotSupportedException(); }
		}

		#endregion

		public long this[long index] {
			get {
				file.Position = index * 8;
				return file.ReadInt64();
			}
		}

		public long RemoveAt(long index) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

			if (index >= 0 && index < Count) {
				file.Position = index * 8;
				// Read the value then remove it
				long ret_val = file.ReadInt64();
				file.Shift(-8);
				// Return the value
				return ret_val;
			}
			
			throw new ArgumentOutOfRangeException();
		}

		public long SearchFirst(long value, IIndexedObjectComparer<long> c) {
			long low = 0;
			long high = Count - 1;

			return SearchFirst(value, c, low, high);
		}

		public long SearchLast(long value, IIndexedObjectComparer<long> c) {
			long low = 0;
			long high = Count - 1;

			return SearchLast(value, c, low, high);
		}

		public bool RemoveSort(long item) {
			return Remove(item, item, comparer);
		}

		public bool Remove(long value, long reference, IIndexedObjectComparer<long> c) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

			// Search for the position of the last value in the set, 
			long[] res = new long[2];
			SearchFirstAndLast(value, c, res);
			long p1 = res[0];
			long p2 = res[1];
			if (p1 < 0)
				throw new ApplicationException("Value '" + value + "' was not found in the set.");

			INumberEnumerator i = GetEnumerator(p1, p2);
			while (i.MoveNext()) {
				// Does the next value match the reference we are looking for?
				if (i.Current == reference) {
					// Remove the value and return
					i.Remove();
					return true;
				}
			}

			return false;
		}

		private class NumberEnumerator : INumberEnumerator {
			public NumberEnumerator(NumberList list, long start, long end) {
				this.list = list;
				this.start = start;
				this.end = end;
			}

			private readonly NumberList list;
			private readonly long start;
			private long end;
			private long pos;
			private int last_op;

			#region Implementation of IDisposable

			public long Count {
				get { return (end - start) + 1; }
			}

			public long Position {
				get { return pos; }
				set {
					last_op = 0;
					pos = value;
				}
			}

			public bool MovePrevious() {
				last_op = 2;
				return --pos > 0;
			}

			public long Remove() {
				long v;
				list.file.Position = (start + pos) * 8;
				v = list.file.ReadInt64();
				list.file.Shift(-8);

				if (last_op == 1)
					--pos;

				--end;

				// Returns the value we removed,
				return v;

			}

			void IInteractiveEnumerator<long>.Remove() {
				Remove();
			}

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				last_op = 1;
				return (start + ++pos < end);
			}

			public void Reset() {
				//TODO:
			}

			public long Current {
				get {
					// Check the point is within the bounds of the iterator,
					if (pos < 0 || start + pos > end)
						throw new IndexOutOfRangeException();

					// Change the position and fetch the data,
					list.file.Position = (start + pos) * 8;
					return list.file.ReadInt64();
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion

			#region Implementation of ICloneable

			public object Clone() {
				return new NumberEnumerator(list, start, end);
			}

			#endregion
		}

		public INumberEnumerator GetEnumerator(long start, long end) {
			// Make sure start and end aren't out of bounds
			if (start < 0 || end > Count || start - 1 > end)
				throw new IndexOutOfRangeException();

			return new NumberEnumerator(this, start, end);
		}

		public INumberEnumerator GetEnumerator() {
			return GetEnumerator(0, Count - 1);
		}

		public void InsertSort(long item) {
			Insert(item, item, comparer);
		}

		public void Insert(long index, long item) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

			if (index >= 0) {
				long sz = Count;
				// Shift and insert
				if (index < sz) {
					file.Position = index * 8;
					file.Shift(8);
					file.Write(item);
					return;
				}
				// Insert at end
				if (index == sz) {
					file.Position = sz * 8;
					file.Write(item);
					return;
				}
			}

			throw new ArgumentOutOfRangeException("index");
		}

		public void Insert(long value, long item, IIndexedObjectComparer<long> c) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

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
			file.Position = pos * 8;
			file.Shift(8);
			file.Write(item);
		}

		public bool InsertUnique(long value, long item, IIndexedObjectComparer<long> c) {
			// If immutable then generate an exception
			if (immutable)
				throw new ApplicationException("Source is immutable.");

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
			file.Position = pos * 8;
			file.Shift(8);
			file.Write(item);
			// Return true because we changed the list,
			return true;
		}

		public bool ContainsSort(long item) {
			return SearchFirst(item, comparer) >= 0;
		}
	}
}