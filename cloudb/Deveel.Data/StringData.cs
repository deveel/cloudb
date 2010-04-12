using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class StringData : IEnumerable<char> {
		public StringData(DataFile file) {
			this.file = file;
		}

		private readonly DataFile file;

		private long CharCount {
			get { return file.Length/2; }
		}

		public long Length {
			get { return CharCount; }
		}

		internal char ReadChar() {
			return file.ReadChar();
		}

		internal void Write(char value) {
			file.Write(value);
		}

		internal void SetPosition(long pos) {
			file.Position = pos * 2;
		}

		internal void SetLength(long length) {
			file.SetLength(length * 2);
		}

		private void DoWriteString(long pos, string str) {
			int len = str.Length;
			// Position and write the characters
			SetPosition(pos);
			for (int i = 0; i < len; ++i)
				file.Write(str[i]);
		}

		private string ReadString(long pos, int sz) {
			StringBuilder buf = new StringBuilder();
			SetPosition(pos);
			for (int i = 0; i < sz; ++i)
				buf.Append(file.ReadChar());
			return buf.ToString();
		}

		public void Append(string str) {
			// Set the position to write the string
			long pos = CharCount;
			SetLength(pos + str.Length);
			// And write the data
			DoWriteString(pos, str);
		}

		public void Insert(long pos, string str) {
			// The length
			int len = str.Length;
			// shift the data area by the length of the string
			SetPosition(pos);
			file.Shift(len * 2);
			// and write the string
			DoWriteString(pos, str);
		}

		public void Remove(long pos, long size) {
			// Some checks
			long data_size = CharCount;
			Debug.Assert(pos >= 0 && size >= 0 && pos + size < data_size);

			SetPosition(pos + size);
			file.Shift(-(size * 2));
		}

		public string Substring(long pos, int size) {
			// Some checks
			long data_size = CharCount;
			Debug.Assert(pos >= 0 && size >= 0 && pos + size < data_size);

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
					return data.file.ReadChar();
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion
		}
	}
}