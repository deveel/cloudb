using System;

using Deveel.Data.Util;

namespace Deveel.Data.Store {
	public abstract class DataFile {
		public abstract long Length { get; }

		public abstract long Position { get; set; }

		public int ReadByte() {
			byte[] buffer = new byte[1];
			int count = Read(buffer, 0, 1);
			if (count == 0)
				return -1;
			return buffer[0];
		}

		public short ReadInt16() {
			byte[] buffer = new byte[2];
			Read(buffer, 0, 2);
			return ByteBuffer.ReadInt2(buffer, 0);
		}

		public int ReadInt32() {
			byte[] buffer = new byte[4];
			Read(buffer, 0, 4);
			return ByteBuffer.ReadInt4(buffer, 0);
		}

		public long ReadInt64() {
			byte[] buffer = new byte[8];
			Read(buffer, 0, 8);
			return ByteBuffer.ReadInt8(buffer, 0);
		}

		public char ReadChar() {
			byte[] buffer = new byte[2];
			Read(buffer, 0, 2);
			return ByteBuffer.ReadChar(buffer, 0);
		}

		public abstract int Read(byte[] buffer, int offset, int count);

		public void Write(byte value) {
			byte[] buffer = new byte[] {value};
			Write(buffer, 0, 1);
		}

		public void Write(short value) {
			byte[] buffer = new byte[2];
			ByteBuffer.WriteInt2(value, buffer, 0);
			Write(buffer, 0, 2);
		}

		public void Write(int value) {
			byte[] buffer = new byte[4];
			ByteBuffer.WriteInteger(value, buffer, 0);
			Write(buffer, 0, 4);
		}

		public void Write(long value) {
			byte[] buffer = new byte[8];
			ByteBuffer.WriteInt8(value, buffer, 0);
			Write(buffer, 0, 8);
		}

		public void Write(char value) {
			byte[] buffer = new byte[2];
			ByteBuffer.WriteChar(value, buffer, 0);
			Write(buffer, 0, 2);
		}

		public abstract void Write(byte[] buffer, int offset, int count);

		public abstract void SetLength(long value);

		public abstract void Shift(long offset);

		public abstract void Delete();

		public abstract void CopyTo(DataFile destFile, long size);
	}
}