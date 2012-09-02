using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Deveel.Data {
	/// <summary>
	/// Represents container of characters, to provide mutable string data,
	/// wrapped on a given <see cref="IDataFile"/>.
	/// </summary>
	/// <remarks>
	/// This class provides efficient methods for retrieving and modifying
	/// strings (seen as sequence of characters).
	/// <para>
	/// Each character is stored in the UTF-16 encoding.
	/// </para>
	/// </remarks>
	public sealed class StringData : IEnumerable<char> {
		/// <summary>
		/// Constructs the string, wrapped around the given <see cref="IDataFile"/>.
		/// </summary>
		/// <param name="file">The data file that wraps this string data container.</param>
		public StringData(IDataFile file) {
			this.file = file;

			fileReader = new BinaryReader(new DataFileStream(file), Encoding.Unicode);
			fileWriter = new BinaryWriter(new DataFileStream(file), Encoding.Unicode);
		}

		private readonly IDataFile file;
		private readonly BinaryReader fileReader;
		private readonly BinaryWriter fileWriter;

		/// <summary>
		/// Gets the number of characters stored in this string.
		/// </summary>
		private long CharCount {
			get { return file.Length/2; }
		}

		/// <summary>
		/// Gets the length of the string.
		/// </summary>
		public long Length {
			get { return CharCount; }
		}

		internal char ReadChar() {
			return fileReader.ReadChar();
		}

		internal void Write(char value) {
			fileWriter.Write(value);
		}

		internal void SetPosition(long pos) {
			file.Position = pos * 2;
		}

		internal void SetLength(long length) {
			file.SetLength(length * 2);
		}

		/// <summary>
		/// Writes the given string starting at the given position.
		/// </summary>
		/// <param name="pos">The position within the container where to
		/// start writing the string.</param>
		/// <param name="str">The string to write.</param>
		/// <remarks>
		/// This method expands where necessary the size of the
		/// underlying data file to accomodate the given string.
		/// </remarks>
		private void DoWriteString(long pos, string str) {
			int len = str.Length;
			// Position and write the characters
			SetPosition(pos);
			for (int i = 0; i < len; ++i)
				fileWriter.Write(str[i]);
		}

		/// <summary>
		/// Reads a string from the container starting at the
		/// given position, given a number of characters.
		/// </summary>
		/// <param name="pos">The position within the container from where
		/// to start reading.</param>
		/// <param name="sz">The number of characters to read.</param>
		/// <returns>
		/// Returns a <see cref="string" /> object that is the result
		/// of the read.
		/// </returns>
		private string ReadString(long pos, int sz) {
			StringBuilder buf = new StringBuilder();
			SetPosition(pos);
			for (int i = 0; i < sz; ++i)
				buf.Append(fileReader.ReadChar());
			return buf.ToString();
		}

		/// <summary>
		/// Appends a string to the end of the data.
		/// </summary>
		/// <param name="str">The string to apopend</param>
		/// <remarks>
		/// This method expands, where necessary, the size of the
		/// wrapped <see cref="IDataFile"/> to accomodate the given
		/// string appended.
		/// </remarks>
		public void Append(string str) {
			// Set the position to write the string
			long pos = CharCount;
			SetLength(pos + str.Length);
			// And write the data
			DoWriteString(pos, str);
		}

		/// <summary>
		/// Inserts the given string to the data, starting at the given position.
		/// </summary>
		/// <param name="pos">The zero-based character position from where
		/// to start inserting the given string.</param>
		/// <param name="str">The string to insert.</param>
		/// <remarks>
		/// This method will shift all the data present between the starting position
		/// of the insert and the length of the string to insert.
		/// <para>
		/// Where necessary, the size underlying data file will be increased to
		/// accomodate the string to insert.
		/// </para>
		/// </remarks>
		public void Insert(long pos, string str) {
			// The length
			int len = str.Length;
			// shift the data area by the length of the string
			SetPosition(pos);
			file.Shift(len * 2);
			// and write the string
			DoWriteString(pos, str);
		}

		/// <summary>
		/// Removes a portion of string data at the given coordinates.
		/// </summary>
		/// <param name="pos">The zero-based character position from where to
		/// start removing the data.</param>
		/// <param name="size">The number of characters to remove.</param>
		public void Remove(long pos, long size) {
			// Some checks
			long dataSize = CharCount;
			Debug.Assert(pos >= 0 && size >= 0 && pos + size < dataSize);

			SetPosition(pos + size);
			file.Shift(-(size * 2));
		}

		/// <summary>
		/// Gets a substring of the current string data starting at the
		/// given offset and having the given size.
		/// </summary>
		/// <param name="pos">The zero-based character position from where 
		/// to start extracting the sub-string.</param>
		/// <param name="size">The length of the desired substring.</param>
		/// <returns></returns>
		public string Substring(long pos, int size) {
			// Some checks
			long dataSize = CharCount;
			if (!(pos >= 0 && size >= 0 && pos + size <= dataSize))
				throw new ArgumentOutOfRangeException();

			return ReadString(pos, size);
		}

		public override string ToString() {
			return ReadString(0, (int)CharCount);
		}

		public string ToString(long start, int count) {
			return ReadString(start, count);
		}

		public IEnumerator<char> GetEnumerator() {
			return GetEnumerator(0, Length);
		}

		public IEnumerator<char> GetEnumerator(long start, long end) {
			return new CharEnumerator(this, start, end);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		private class CharEnumerator : IEnumerator<char> {
			private readonly StringData data;
			private readonly long start;
			private readonly long end;
			private long pos;

			public CharEnumerator(StringData data, long start, long end) {
				this.data = data;
				this.end = end;
				this.start = start;
				pos = start - 1;
			}

			#region Implementation of IDisposable

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				return ++pos < end;
			}

			public void Reset() {
				pos = start - 1;
			}

			public char Current {
				get {
					if (pos < start || pos >= end)
						throw new InvalidOperationException();

					data.SetPosition(pos);
					return data.fileReader.ReadChar();
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion
		}
	}
}