using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data {
	public sealed class BinaryCollection : ISortedCollection<Binary> {
		private readonly IDataFile file;
		private readonly IComparer<Binary> comparer;

		private readonly BinaryReader fileReader;
		private readonly BinaryWriter fileWriter;

		private readonly BinaryCollection root;
		private bool rootStateDirty;

		private readonly Binary upperBound;
		private readonly Binary lowerBound;
		private long version;

		private long startPos, endPos;
		private long foundItemStart, foundItemEnd;

		private const int Deliminator = Int16.MaxValue /*0x0FFF8*/;


		private static readonly IComparer<Binary> NaturalComparer = new BinaryComparer();

		private const long Magic = 0x0BE0220F;

		private BinaryCollection(IDataFile file, IComparer<Binary> comparer, Binary lowerBound, Binary upperBound) {
			this.file = file;

			fileReader = new BinaryReader(new DataFileStream(file), Encoding.Unicode);
			fileWriter = new BinaryWriter(new DataFileStream(file), Encoding.Unicode);

			if (comparer == null)
				comparer = NaturalComparer;

			this.comparer = comparer;

			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}

		private BinaryCollection(BinaryCollection collection, Binary lowerBound, Binary upperBound)
			: this(collection.file, collection.comparer, lowerBound, upperBound) {
			root = collection;
			version = -1;
		}


		public BinaryCollection(IDataFile file, IComparer<Binary> comparer)
			: this(file, comparer, null, null) {
			version = 0;
			root = this;
			rootStateDirty = true;
		}

		public BinaryCollection(IDataFile file)
			: this(file, null) {
		}

		private void UpdateInternalState() {
			if (version < root.version || rootStateDirty) {
				// Reset the root state dirty boolean
				rootStateDirty = false;

				// Read the size
				long sz = file.Length;

				// The empty states,
				if (sz < 8) {
					startPos = 0;
					endPos = 0;
				} else if (sz == 8) {
					startPos = 8;
					endPos = 8;
				}
					// The none empty state
				else {

					// If there is no lower bound we use start of the list
					if (lowerBound == null) {
						startPos = 8;
					}
						// If there is a lower bound we search for the binary and use it
					else {
						SearchFor(lowerBound, 8, sz);
						startPos = file.Position;
					}

					// If there is no upper bound we use end of the list
					if (upperBound == null) {
						endPos = sz;
					}
						// Otherwise there is an upper bound so search for the binary and use it
					else {
						SearchFor(upperBound, 8, sz);
						endPos = file.Position;
					}
				}

				// Update the version of this to the parent.
				version = root.version;
			}
		}

		private Binary BinaryAt(long s, long e) {
			long toRead = (e - s) - 4;
			// If it's too large
			if (toRead > Int32.MaxValue)
				throw new ApplicationException("Binary too large to read.");

			file.Position = s + 2;

			// Decode to a buffer,
			int leftToRead = (int) toRead;
			byte[] buf = new byte[leftToRead];
			int pos = 0;
			bool lastWasEscape = false;

			while (leftToRead > 0) {
				int read = Math.Min(128, leftToRead);
				fileReader.Read(buf, pos, read);

				// Decode,
				int readTo = pos + read;
				for (; pos < readTo; pos += 2) {
					if (buf[pos] == (byte) 0x0FF &&
					    buf[pos + 1] == (byte) 0x0F8) {
						// This is the escape sequence,
						if (lastWasEscape) {
							// Ok, we convert this to a single,

							Array.Copy(buf, pos + 2, buf, pos, readTo - pos - 2);
							pos -= 2;
							readTo -= 2;
							lastWasEscape = false;
						} else {
							lastWasEscape = true;
							continue;
						}
					}
					// This is illegal. The binary contained 0x0FFF8 that was not followed
					// by 0x0FFF8.
					if (lastWasEscape) {
						throw new ApplicationException("Encoding error");
					}
				}

				leftToRead -= read;
			}

			// Illegal, must end with 'last_was_escape' at false.
			if (lastWasEscape) {
				throw new ApplicationException("Encoding error");
			}

			// 'buf' now contains the decoded sequence. Turn it into a ByteArray
			// object.

			byte lastByte = buf[pos - 1];
			if (lastByte == 0x00) {
				// Last byte is 0 indicate we lose 1 byte from the end,
				pos -= 1;
			} else if (lastByte == 0x01) {
				// Last byte of 1 indicates we lose 2 bytes from the end.
				pos -= 2;
			} else {
				throw new ApplicationException("Encoding error");
			}

			return new Binary(buf, 0, pos);
		}

		private void RemoveCurrent() {
			// Tell the root set that any child subsets may be dirty
			if (root != this) {
				root.version += 1;
				root.rootStateDirty = true;
			}
			version += 1;

			// The number of byte entries to remove
			long binRemoveSize = foundItemStart - foundItemEnd;
			file.Position = foundItemEnd;
			file.Shift(binRemoveSize);
			endPos = endPos + binRemoveSize;

			// If this removal leaves the set empty, we delete the file and update the
			// internal state as necessary.
			if (startPos == 8 && endPos == 8) {
				file.Delete();
				startPos = 0;
				endPos = 0;
			}
		}

		private void InsertValue(Binary value) {
			// This encodes the value and stores it at the position. The encoding
			// process looks for 0x0FFF8 sequences and encodes it as a pair. This
			// allows to distinguish between a 0x0FFF8 seqence in the binary data and
			// a record deliminator.

			// Tell the root set that any child subsets may be dirty
			if (root != this) {
				root.version += 1;
				root.rootStateDirty = true;
			}

			version += 1;

			// If the set is empty, we insert the magic value to the start of the
			// data file and update the internal vars as appropriate
			if (file.Length < 8) {
				file.SetLength(8);
				file.Position = 0;
				fileWriter.Write(Magic);
				startPos = 8;
				endPos = 8;
			}

			// Encode the value,
			int len = value.Length;
			// Make enough room to store the value and round up to the nearest word
			int actLen;
			if (len % 2 == 0) actLen = len + 2;
			else actLen = len + 1;

			// Note that this is an estimate. Any 'FFF8' sequences found will expand
			// the file when found.
			file.Shift(actLen + 4);
			fileWriter.Write((short)0);
			int i = 0;
			int readLen = (len / 2) * 2;
			for (; i < readLen; i += 2) {
				byte b1 = value[i];
				byte b2 = value[i + 1];
				fileWriter.Write(b1);
				fileWriter.Write(b2);
				// 'FFF8' sequence will write itself again.
				if (b1 == 0x0FF && b2 == 0x0F8) {
					file.Shift(2);
					fileWriter.Write((byte)0x0FF);
					fileWriter.Write((byte)0x0F8);
					actLen += 2;
				}
			}

			// Put tail characters,
			if (i == len) {
				fileWriter.Write((byte)0x00);
				fileWriter.Write((byte)0x01);
			} else {
				fileWriter.Write(value[i]);
				fileWriter.Write((byte)0x00);
			}

			// Write the deliminator
			fileWriter.Write((short)Deliminator);

			// Adjust end_pos
			endPos = endPos + (actLen + 4);
		}

		private long ScanForEnd(long pos, long start, long end) {
			file.Position = pos;

			long initPos = pos;

			// Did we land on a deliminator sequence?
			if (pos < end && fileReader.ReadInt16() == Deliminator) {
				// If there's not an escape char after or before then we did,
				if (pos + 2 >= end || fileReader.ReadInt16() != Deliminator) {
					file.Position = pos - 2;
					if (pos - 2 < start || fileReader.ReadInt16() != Deliminator) {
						// Ok, this is deliminator. The char before and after are not 0x0FFF8
						return pos + 2;
					}
				}
				// If we are here we landed on a deliminator that is paired, so now
				// scan forward until we reach the last,
				pos = pos + 2;
				file.Position = pos;
				while (pos < end) {
					pos = pos + 2;
					if (fileReader.ReadInt16() != Deliminator) {
						break;
					}
				}
			}

			file.Position = pos;

			while (pos < end) {
				short c = fileReader.ReadInt16();
				pos = pos + 2;
				if (c == Deliminator) {
					// This is the end of the binary if the 0x0FFF8 sequence is on its own.
					if (pos >= end || fileReader.ReadInt16() != Deliminator) {
						// This is the end of the binary, break the while loop
						return pos;
					}

					// Not end because 0x0FFF8 is repeated,
					pos = pos + 2;
				}
			}

			// All bins must end with 0x0FFF8.  If this character isn't found before
			// the end is reached then the format of the data is in error.

			file.Position = start;
			for (long i = start; i < end; ++i) {
				byte b = fileReader.ReadByte();
			}

			throw new ApplicationException("Set data error.");
		}

		private long ScanForStart(long pos, long start, long end) {
			file.Position = pos;

			// Did we land on a deliminator sequence?
			if (pos < end && fileReader.ReadInt16() == Deliminator) {
				// If there's not an escape char after or before then we did,
				if (pos + 2 >= end || fileReader.ReadInt16() != Deliminator) {
					file.Position = pos - 2;
					if (pos - 2 < start || fileReader.ReadInt16() != Deliminator) {
						// Ok, this is deliminator. The char before and after are not 0x0FFF8
						return pos + 2;
					}
				}
				// If we are here we landed on a deliminator that is paired, so now
				// scan backward until we reach the first none deliminated entry,
				pos = pos - 2;
				while (pos >= start) {
					file.Position = pos;
					if (fileReader.ReadInt16() != Deliminator) {
						break;
					}
					pos = pos - 2;
				}
			}

			while (pos >= start) {
				file.Position = pos;
				short c = fileReader.ReadInt16();
				pos = pos - 2;
				if (c == Deliminator) {
					// This is the end of the binary if the 0x0FFF8 sequence is on its own.
					file.Position=pos;
					if (pos < start || fileReader.ReadInt16() != Deliminator)
						// This is the end of the binary, break the while loop
						return pos + 4;

					// Not end because 0x0FFF8 is repeated,
					pos = pos - 2;
				}
			}

			// We hit the start of the bounded area,
			return pos + 2;
		}

		private bool SearchFor(Binary value, long start, long end) {
			// If start is end, the list is empty,
			if (start == end) {
				file.Position = start;
				return false;
			}

			// How large is the area we are searching in characters?
			long searchLen = (end - start)/2;
			// Read the binary from the middle of the area
			long midPos = start + ((searchLen/2)*2);

			// Search to the end of the binary
			long binEnd = ScanForEnd(midPos, start, end);
			long binStart = ScanForStart(midPos - 2, start, end);

			// Now binStart will point to the start of the binary and binEnd to the
			// end (the byte immediately after 0x0FFFF).
			// Read the midpoint binary,
			Binary midValue = BinaryAt(binStart, binEnd);

			// Compare the values
			int v = comparer.Compare(value, midValue);
			// If str_start and str_end are the same as start and end, then the area
			// we are searching represents only 1 binary, which is a return state
			bool lastBin = (binStart == start && binEnd == end);

			if (v < 0) {
				// if value < mid_value
				if (lastBin) {
					// Position at the start if last str and value < this value
					file.Position = binStart;
					return false;
				}
				// We search the head
				return SearchFor(value, start, binStart);
			}
			
			if (v > 0) {
				// if value > mid_value
				if (lastBin) {
					// Position at the end if last str and value > this value
					file.Position = binEnd;
					return false;
				}
				// We search the tail
				return SearchFor(value, binEnd, end);
			}

			// if value == mid_value
			file.Position = binStart;
			// Update internal state variables
			foundItemStart = binStart;
			foundItemEnd = binEnd;
			return true;
		}

		private Binary Bounded(Binary binary) {
			if (binary == null)
				throw new ArgumentNullException("binary");

			// If str is less than lower bound then return lower bound
			if (lowerBound != null &&
				comparer.Compare(binary, lowerBound) < 0) {
				return lowerBound;
			}
			// If str is greater than upper bound then return upper bound
			if (upperBound != null &&
				comparer.Compare(binary, upperBound) >= 0) {
				return upperBound;
			}
			return binary;
		}


		public IEnumerator<Binary> GetEnumerator() {
			UpdateInternalState();
			return new BinaryEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		void ICollection<Binary>.Add(Binary item) {
			Add(item);
		}

		public bool Add(Binary item) {
			if (item == null)
				throw new ArgumentNullException("item");

			// As per the contract, this method can not add values that compare below
			// the lower bound or compare equal or greater to the upper bound.
			if (lowerBound != null &&
				comparer.Compare(item, lowerBound) < 0) {
				throw new ArgumentException("value < lower_bound");
			}
			if (upperBound != null &&
				comparer.Compare(item, upperBound) >= 0) {
				throw new ArgumentException("value >= upper_bound");
			}

			UpdateInternalState();

			// Find the index in the list of the value either equal to the given value
			// or the first value in the set comparatively more than the given value.
			bool found = SearchFor(item, startPos, endPos);
			// If the value was found,
			if (found)
				// Return false
				return false;

			// Not found, so insert into the set at the position we previously
			// discovered.
			InsertValue(item);
			// And return true
			return true;
		}

		public bool Replace( Binary value) {
			if (value == null)
				throw new ArgumentNullException("value");

			// As per the contract, this method can not replace values that compare
			// below the lower bound or compare equal or greater to the upper bound.
			if (lowerBound != null &&
				comparer.Compare(value, lowerBound) < 0) {
				throw new ArgumentException("value < lower_bound");
			}
			if (upperBound != null &&
				comparer.Compare(value, upperBound) >= 0) {
				throw new ArgumentException("value >= upper_bound");
			}

			UpdateInternalState();

			// Find the index in the list of the value either equal to the given value
			// or the first value in the set comparatively more than the given value.
			bool found = SearchFor(value, startPos, endPos);
			// If the value was not found,
			if (!found)
				// Return false
				return false;

			// Found, so remove and then insert a new value,
			RemoveCurrent();
			// Reposition to the start of the delete area
			if (file.Length == 0) {
				// Removed last entry so set the position at the start,
				file.Position = 0;
			} else {
				// Otherwise set position to the start of the item,
				file.Position = foundItemStart;
			}

			InsertValue(value);

			// And return true
			return true;
		}

		public void ReplaceOrAdd(Binary value) {
			if (value == null)
				throw new ArgumentNullException("value");

			// As per the contract, this method can not replace values that compare
			// below the lower bound or compare equal or greater to the upper bound.
			if (lowerBound != null &&
				comparer.Compare(value, lowerBound) < 0) {
				throw new ArgumentException("value < lower_bound");
			}

			if (upperBound != null &&
				comparer.Compare(value, upperBound) >= 0) {
				throw new ArgumentException("value >= upper_bound");
			}

			UpdateInternalState();

			// Find the index in the list of the value either equal to the given value
			// or the first value in the set comparatively more than the given value.
			bool found = SearchFor(value, startPos, endPos);
			// If the value was not found,
			if (!found) {
				// Add to the list,
				InsertValue(value);
			} else {
				// Found, so remove and then insert a new value,
				RemoveCurrent();
				// Reposition to the start of the delete area
				if (file.Length == 0) {
					// Removed last entry so set the position at the start,
					file.Position = 0;
				} else {
					// Otherwise set position to the start of the item,
					file.Position = foundItemStart;
				}

				InsertValue(value);
			}
		}

		public void Clear() {
			UpdateInternalState();

			// Tell the root set that any child subsets may be dirty
			if (root != this) {
				root.version += 1;
				root.rootStateDirty = true;
			}

			version += 1;

			// Clear the list between the start and end,
			long toClear = startPos - endPos;
			file.Position = endPos;
			file.Shift(toClear);
			endPos = startPos;

			// If it's completely empty, we delete the file,
			if (startPos == 8 && endPos == 8) {
				file.Delete();
				startPos = 0;
				endPos = 0;
			}
		}

		public bool Contains(Binary item) {
			if (item == null)
				throw new ArgumentNullException("item");

			UpdateInternalState();

			// Look for the binary in the file.
			return SearchFor(item, startPos, endPos);

		}

		public void CopyTo(Binary[] array, int arrayIndex) {
			if (array == null)
				throw new ArgumentNullException("array");

			int toCopy = array.Length - arrayIndex;
			IEnumerator<Binary> enumerator = GetEnumerator();
			int i = 0;
			while (i < toCopy && enumerator.MoveNext()) {
				array[i] = enumerator.Current;
				i++;
			}

		}

		public bool Remove(Binary item) {
			if (item == null)
				throw new ArgumentNullException("item");

			UpdateInternalState();
			// Find the index in the list of the value either equal to the given value
			// or the first value in the set comparatively more than the given value.
			bool found = SearchFor(item, startPos, endPos);
			// If the value was found,
			if (found) {
				// Remove it
				RemoveCurrent();
			}
			return found;
		}

		public int Count {
			get {
				UpdateInternalState();
				long p = startPos;
				long end = endPos;
				int count = 0;
				while (p < end && count < Int32.MaxValue) {
					++count;
					p = ScanForEnd(p, p, end);
				}

				return count;
			}
		}

		bool ICollection<Binary>.IsReadOnly {
			get { return false; }
		}

		public IComparer<Binary> Comparer {
			get { return comparer; }
		}

		public Binary First {
			get {
				UpdateInternalState();

				if (startPos >= endPos)
					throw new NullReferenceException();

				long foundEnd = ScanForEnd(startPos, startPos, endPos);
				return BinaryAt(startPos, foundEnd);
			}
		}

		public Binary Last {
			get {
				UpdateInternalState();

				if (startPos >= endPos)
					throw new NullReferenceException();

				long foundStart = ScanForStart(endPos - 2, startPos, endPos);
				return BinaryAt(foundStart, endPos);
			}
		}

		public bool IsEmpty {
			get {
				UpdateInternalState();

				// If start_pos == end_pos then the list is empty
				return startPos == endPos;
			}
		}

		public BinaryCollection Tail(Binary start) {
			return new BinaryCollection(root, Bounded(start), upperBound);
		}

		ISortedCollection<Binary> ISortedCollection<Binary>.Tail(Binary start) {
			return Tail(start);
		}

		ISortedCollection<Binary> ISortedCollection<Binary>.Head(Binary end) {
			return Head(end);
		}

		public BinaryCollection Head(Binary end) {
			return new BinaryCollection(root, lowerBound, Bounded(end));
		}

		ISortedCollection<Binary> ISortedCollection<Binary>.Sub(Binary start, Binary end) {
			return Sub(start, end);
		}

		public BinaryCollection Sub(Binary start, Binary end) {
			// check the bounds not out of range of the parent bounds
			return new BinaryCollection(root, Bounded(start), Bounded(end));			
		}

		#region BinaryComparer

		class BinaryComparer : IComparer<Binary> {
			public int Compare(Binary x, Binary y) {
				return x.CompareTo(y);
			}
		}

		#endregion

		#region BinaryEnumerator

		class BinaryEnumerator : IInteractiveEnumerator<Binary> {
			private readonly BinaryCollection collection;

			// The version of which this is derived,
			private long ver;
			// Offset of the iterator
			private long offset;
			// Last binary position
			private long lastBinStart = -1;
			private long lastBinEnd = -1;

			public BinaryEnumerator(BinaryCollection collection) {
				this.collection = collection;
				ver = collection.root.version;
				offset = 0;
			}

			private void VersionCheck() {
				if (ver < collection.root.version) {
					throw new InvalidOperationException("Concurrent set update");
				}
			}

			public bool MoveNext() {
				VersionCheck();

				if (collection.startPos + offset < collection.endPos) {
					long p = collection.startPos + offset;
					lastBinStart = p;

					lastBinEnd = collection.ScanForEnd(p, p, collection.endPos);
					offset += lastBinEnd - p;

					return true;
				}

				return false;
			}

			public void Reset() {
				ver = collection.root.version;
				offset = 0;
				lastBinStart = -1;
				lastBinEnd = -1;
			}

			public Binary Current {
				get {
					VersionCheck();
					return collection.BinaryAt(lastBinStart, lastBinEnd);
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public void Remove() {
				VersionCheck();
				if (lastBinStart == -1)
					throw new InvalidOperationException();

				collection.foundItemStart = lastBinStart;
				collection.foundItemEnd = collection.startPos + offset;
				// Remove the binary
				collection.RemoveCurrent();
				// Update internal state
				offset = lastBinStart - collection.startPos;
				lastBinStart = -1;
				// Update the version of this iterator
				ver = ver + 1;
			}

			public void Dispose() {
			}
		}

		#endregion
	}
}