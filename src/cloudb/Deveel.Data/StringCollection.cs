using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data {
	public sealed class StringCollection : ISortedCollection<string> {
		public StringCollection(IDataFile file, IComparer<string> comparer)
			: this(file, comparer, null, null) {
			version = 0;
			root = this;
			rootStateDirty = true;
		}

		public StringCollection(IDataFile file)
			: this(file, null) {
		}

		private StringCollection(IDataFile file, IComparer<string> comparer, string lowerBound, string upperBound) {
			this.file = file;

			fileReader = new BinaryReader(new DataFileStream(file), Encoding.Unicode);
			fileWriter = new BinaryWriter(new DataFileStream(file), Encoding.Unicode);

			if (comparer == null)
				comparer = DefaultComparer;

			this.comparer = comparer;

			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}

		private StringCollection(StringCollection collection, string lowerBound, string upperBound)
			: this(collection.file, collection.comparer, lowerBound, upperBound) {
			root = collection;
			version = -1;
		}

		private readonly IDataFile file;
		private readonly IComparer<string> comparer;

		private readonly BinaryReader fileReader;
		private readonly BinaryWriter fileWriter;

		private readonly StringCollection root;
		private bool rootStateDirty;

		private readonly string upperBound;
		private readonly string lowerBound;
		private long version;

		private long startPos, endPos;
		private long foundItemStart, foundItemEnd;

		private readonly IComparer<String> DefaultComparer = new LexiconComparer();

		private const char StringDeliminator = (char) 0x0FFFF;
		private const long Magic = 0x0BE0110F;

		public IComparer<string> Comparer {
			get { return comparer; }
		}

		public string First {
			get {
				UpdateInternalState();

				if (startPos >= endPos)
					throw new NullReferenceException();

				// Get the first entry
				file.Position = startPos;
				StringBuilder str_buf = new StringBuilder();
				while (true) {
					char c = fileReader.ReadChar();
					if (c == StringDeliminator)
						break;

					str_buf.Append(c);
				}
				return str_buf.ToString();
			}
		}

		public string Last {
			get {
				UpdateInternalState();

				if (startPos >= endPos)
					throw new NullReferenceException();

				// Get the last entry
				long p = endPos - 2;
				while (p > startPos) {
					file.Position = p;
					char c = fileReader.ReadChar();
					if (c == StringDeliminator) {
						p = p + 2;
						break;
					}

					p = p - 2;
				}
				return ReadString(p, endPos);
			}
		}

		private class LexiconComparer : IComparer<string> {
			#region Implementation of IComparer<string>

			public int Compare(string x, string y) {
				return x.CompareTo(y);
			}

			#endregion
		}

		private string Bounded(String str) {
			if (str == null)
				throw new ArgumentNullException("str");

			// If str is less than lower bound then return lower bound
			if (lowerBound != null &&
				Comparer.Compare(str, lowerBound) < 0)
				return lowerBound;

			// If str is greater than upper bound then return upper bound
			if (upperBound != null &&
				Comparer.Compare(str, upperBound) >= 0)
				return upperBound;

			return str;
		}

		private void UpdateInternalState() {
			if (version < root.version || 
				rootStateDirty) {
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
						// If there is a lower bound we search for the string and use it
					else {
						SearchFor(lowerBound, 8, sz);
						startPos = file.Position;
					}

					// If there is no upper bound we use end of the list
					if (upperBound == null) {
						endPos = sz;
					}
						// Otherwise there is an upper bound so search for the string and use it
					else {
						SearchFor(upperBound, 8, sz);
						endPos = file.Position;
					}
				}

				// Update the version of this to the parent.
				version = root.version;
			}
		}

		private void RemoveAtPosition() {
			// Tell the root set that any child subsets may be dirty
			if (root != this) {
				root.version += 1;
				root.rootStateDirty = true;
			}

			version += 1;

			// The number of byte entries to remove
			long strRemoveSize = foundItemStart - foundItemEnd;
			file.Position = foundItemEnd;
			file.Shift(strRemoveSize);
			endPos = endPos + strRemoveSize;

			// If this removal leaves the set empty, we delete the file and update the
			// internal state as necessary.
			if (startPos == 8 && endPos == 8) {
				file.Delete();
				startPos = 0;
				endPos = 0;
			}
		}

		private String ReadString(long s, long e) {
			long toRead = ((e - s) - 2) / 2;
			// If it's too large
			if (toRead > Int32.MaxValue)
				throw new ApplicationException("String too large to read.");

			file.Position = s;
			int sz = (int)toRead;
			StringBuilder buf = new StringBuilder(sz);
			for (int i = 0; i < sz; ++i)
				buf.Append(fileReader.ReadChar());

			// Returns the string
			return buf.ToString();
		}

		private void InsertValue(String value) {
			// The string encoding is fairly simple.  Each short represents a
			// UTF-16 encoded character of the string.  We use 0x0FFFF to represent
			// the string record separator (an invalid UTF-16 character) at the end
			// of the string.  This method will not permit a string to be inserted
			// that contains a 0x0FFFF character.

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

			int len = value.Length;
			// Make room in the file for the value being inserted
			long strInsertSize = ((long)len * 2) + 2;
			long curPosition = file.Position;
			file.Shift(strInsertSize);
			// Change the end position
			endPos = endPos + strInsertSize;

			// Insert the characters
			for (int i = 0; i < len; ++i) {
				char c = value[i];
				// Check if the character is 0x0FFFF, if it is then generate an error
				if (c == StringDeliminator) {
					// Revert any changes we made
					file.Position = curPosition + strInsertSize;
					file.Shift(-strInsertSize);
					endPos = endPos - strInsertSize;
					// Throw a runtime exception (don't throw an IO exception because
					// this will cause a critical stop condition).
					throw new ApplicationException("Can not encode invalid UTF-16 character 0x0FFFF");
				}
				fileWriter.Write(c);
			}

			// Write the string deliminator
			fileWriter.Write(StringDeliminator);
		}

		private bool SearchFor(string value, long start, long end) {
			// If start is end, the list is empty,
			if (start == end) {
				file.Position = start;
				return false;
			}

			// How large is the area we are searching in characters?
			long searchLen = (end - start)/2;
			// Read the string from the middle of the area
			long midPos = start + ((searchLen/2)*2);
			// Search to the end of the string
			long strEnd = -1;
			long pos = midPos;
			file.Position = pos;
			while (pos < end) {
				char c = fileReader.ReadChar();
				pos = pos + 2;
				if (c == StringDeliminator) {
					// This is the end of the string, break the while loop
					strEnd = pos;
					break;
				}
			}
			// All strings must end with 0x0FFFF.  If this character isn't found before
			// the end is reached then the format of the data is in error.
			if (strEnd == -1)
				throw new ApplicationException("Collection data error.");

			// Search for the start of the string
			long strStart = midPos - 2;
			while (strStart >= start) {
				file.Position = strStart;
				char c = fileReader.ReadChar();
				if (c == StringDeliminator) {
					// This means we found the end of the previous string
					// so the start is the next char.
					break;
				}
				strStart = strStart - 2;
			}
			strStart = strStart + 2;

			// Now str_start will point to the start of the string and str_end to the
			// end (the char immediately after 0x0FFFF).
			// Read the midpoint string,
			string midValue = ReadString(strStart, strEnd);

			// Compare the values
			int v = comparer.Compare(value, midValue);
			// If str_start and str_end are the same as start and end, then the area
			// we are searching represents only 1 string, which is a return state
			bool lastStr = (strStart == start && strEnd == end);

			if (v < 0) {
				// if value < mid_value
				if (lastStr) {
					// Position at the start if last str and value < this value
					file.Position = strStart;
					return false;
				}
				// We search the head
				return SearchFor(value, start, strStart);
			}
			if (v > 0) {
				// if value > mid_value
				if (lastStr) {
					// Position at the end if last str and value > this value
					file.Position = strEnd;
					return false;
				}
				// We search the tail
				return SearchFor(value, strEnd, end);
			}
			// if value == mid_value
			file.Position = strStart;
			// Update internal state variables
			foundItemStart = strStart;
			foundItemEnd = strEnd;
			return true;
		}

		#region Implementation of IEnumerable

		public IEnumerator<string> GetEnumerator() {
			UpdateInternalState();
			return new StringSetEnumerator(this);
		}

		public class StringSetEnumerator : IInteractiveEnumerator<string> {
			public StringSetEnumerator(StringCollection collection) {
				this.collection = collection;
				version = collection.version;
				offset = 0;
			}

			private readonly StringCollection collection;
			private long version;
			private long offset;
			private long lastStrStart = -1;

			private void CheckVersion() {
				if (version < collection.root.version)
					throw new InvalidOperationException("Concurrent update");
			}

			#region Implementation of IDisposable

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				CheckVersion();
				return collection.startPos + offset < collection.endPos;
			}

			public void Reset() {
				//TODO:
			}

			public string Current {
				get {
					CheckVersion();
					long p = collection.startPos + offset;
					lastStrStart = p;
					collection.file.Position = p;
					StringBuilder buf = new StringBuilder();
					while (true) {
						char c = collection.fileReader.ReadChar();
						offset += 2;
						if (c == StringDeliminator)
							break;

						buf.Append(c);
					}
					return buf.ToString();
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion

			public void Remove() {
				CheckVersion();
				if (lastStrStart == -1)
					throw new InvalidOperationException();

				collection.foundItemStart = lastStrStart;
				collection.foundItemEnd = collection.startPos + offset;

				collection.RemoveAtPosition();

				// Update internal state
				offset = lastStrStart - collection.startPos;
				lastStrStart = -1;

				// Update the version of this iterator
				version = version + 1;
			}

		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#endregion

		#region Implementation of ICollection<string>

		public void Add(string item) {
			if (item == null)
				throw new ArgumentNullException("item");

			if (lowerBound != null &&
			    comparer.Compare(item, lowerBound) < 0)
				throw new ArgumentOutOfRangeException("item");

			if (upperBound != null &&
			    comparer.Compare(item, upperBound) >= 0)
				throw new ArgumentOutOfRangeException("item");

			UpdateInternalState();

			bool found = SearchFor(item, startPos, endPos);
			if (found)
				throw new ArgumentException();

			InsertValue(item);
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

		public bool Contains(string item) {
			if (item == null) 
				throw new ArgumentNullException("item");

			UpdateInternalState();
			// Look for the string in the file.
			return SearchFor(item, startPos, endPos);
		}

		public void CopyTo(string[] array, int arrayIndex) {
			if (array == null)
				throw new ArgumentNullException("array");

			int toCopy = array.Length - arrayIndex;
			IEnumerator<string> enumerator = GetEnumerator();
			int i = 0;
			while (i < toCopy && enumerator.MoveNext()) {
				array[i] = enumerator.Current;
				i++;
			}
		}

		public bool Remove(string item) {
			if (item == null) 
				throw new ArgumentNullException("item");

			UpdateInternalState();

			bool found = SearchFor(item, startPos, endPos);
			if (found)
				RemoveAtPosition();
			return found;
		}

		public int Count {
			get {
				UpdateInternalState();
				int listSize = 0;
				long p = startPos;
				long size = endPos;
				file.Position = p;
				while (listSize < Int32.MaxValue && p < size) {
					char c = fileReader.ReadChar();
					if (c == StringDeliminator)
						++listSize;

					p += 2;
				}
				return listSize;
			}
		}

		public bool IsReadOnly {
			get { return false; }
		}

		public bool IsEmpty {
			get {
				UpdateInternalState();
				// If start_pos == end_pos then the list is empty
				return startPos == endPos;
			}
		}

		#endregion

		ISortedCollection<string> ISortedCollection<string>.Tail(string start) {
			return Tail(start);
		}

		ISortedCollection<string> ISortedCollection<string>.Head(string end) {
			return Head(end);
		}

		ISortedCollection<string> ISortedCollection<string>.Sub(string start, string end) {
			return Sub(start, end);
		}

		public StringCollection Tail(string start) {
			start = Bounded(start);
			return new StringCollection(root, start, upperBound);
		}

		public StringCollection Head(string end) {
			end = Bounded(end);
			return new StringCollection(root, lowerBound, end);
		}

		public StringCollection Sub(string start, string end) {
			start = Bounded(start);
			end = Bounded(end);
			return new StringCollection(root, start, end);
		}
	}
}