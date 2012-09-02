using System;
using System.IO;

namespace Deveel.Data {
	public sealed class Binary : IComparable, IComparable<Binary>, IEquatable<Binary> {
		private readonly byte[] buffer;
		private readonly int offset;
		private readonly int length;

		public Binary(byte[] buffer, int offset, int length) {
			this.buffer = buffer;
			this.offset = offset;
			this.length = length;
		}

		public Binary(byte[] buffer)
			: this(buffer, 0, buffer.Length) {
		}

		public int Length {
			get { return length; }
		}

		public byte this[int index] {
			get { return buffer[offset + index]; }
		}

		public Stream GetInputStream() {
			return new MemoryStream(buffer, offset, length, false);
		}

		public int CompareTo(Binary other) {
			int len1 = length;
			int len2 = other.length;
			int clen = Math.Min(len1, len2);
			for (int i = 0; i < clen; ++i) {
				byte v1 = this[i];
				byte v2 = other[i];
				if (v1 != v2)
					return v1 > v2 ? 1 : -1;
			}

			return len1 - len2;
		}

		public int CompareTo(object obj) {
			Binary other = obj as Binary;
			if (other == null)
				throw new ArgumentException();

			return CompareTo(other);
		}

		public bool Equals(Binary other) {
			if (length != other.length)
				return false;

			int sz = length;
			for (int i = 0; i < sz; ++i) {
				if (this[i] != other[i])
					return false;
			}
			return true;

		}

		public override bool Equals(object obj) {
			Binary other = obj as Binary;
			if (other == null)
				return false;

			return Equals(other);
		}

		public override int GetHashCode() {
			int code = length.GetHashCode();
			for (int i = 0; i < length; i++) {
				code ^= this[i].GetHashCode();
			}
			return code;
		}
	}
}