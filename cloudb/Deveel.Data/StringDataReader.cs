using System;
using System.Diagnostics;
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="TextReader"/> used to read from a given
	/// <see cref="StringData"/> in an efficient forward-only behavior.
	/// </summary>
	public sealed class StringDataReader : TextReader {
		private readonly StringData data;
		private long pos;
		private readonly long end;

		/// <summary>
		/// Constructs the reader on the given string, given a start and end
		/// offset within the object.
		/// </summary>
		/// <param name="data">The string to be read.</param>
		/// <param name="start">The start offset of the read.</param>
		/// <param name="end">The end offset of the read.</param>
		public StringDataReader(StringData data, long start, long end) {
			this.data = data;
			this.end = end;
			pos = start;
		}

		/// <summary>
		/// Constructs the reader on the given string, given a start
		/// offset within the object.
		/// </summary>
		/// <param name="data">The string to be read.</param>
		/// <param name="start">The start offset of the read.</param>
		public StringDataReader(StringData data, long start)
			: this(data, start, data.Length) {
		}

		/// <summary>
		/// Constructs the reader on the given string.
		/// </summary>
		/// <param name="data">The string to be read.</param>
		public StringDataReader(StringData data)
			: this(data, 0) {
		}

		public override int Read() {
			// End of stream reached
			if (pos >= end)
				return -1;

			data.SetPosition(pos);
			++pos;
			return data.ReadChar();
		}

		public override int Read(char[] buffer, int index, int count) {
			Debug.Assert(count >= 0 && index >= 0);
			// As per the contract, if we have reached the end return -1
			if (pos >= end)
				return 0;

			data.SetPosition(pos);
			long actEnd = Math.Min(pos + count, end);
			int toRead = (int)(actEnd - pos);
			for (int i = index; i < index + toRead; ++i)
				buffer[i] = data.ReadChar();

			pos += toRead;
			return toRead;
		}
	}
}