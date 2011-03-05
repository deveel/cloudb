using System;

namespace Deveel.Data {
	/// <summary>
	/// This class is used to represent a flexible structure of binary data 
	/// contained within a database system.
	/// </summary>
	/// <remarks>
	/// The architecture of a <see cref="DataFile"/> is flexible because it
	/// can be grown, shrunk, contained data can be moved, removed, inserted
	/// at arbitrary positions.
	/// <para>
	/// A <see cref="DataFile"/> is constructed by pointing a portion of data
	/// present in a database, making this pointer to advance at every write
	/// operation, or by setting a current position different from the original one.
	/// </para>
	/// </remarks>
	public abstract class DataFile {
		/// <summary>
		/// Gets the length of the portion of data pointed by this file.
		/// </summary>
		public abstract long Length { get; }

		/// <summary>
		/// Gets or sets the current position of the pointer.
		/// </summary>
		public abstract long Position { get; set; }

		/// <summary>
		/// Reads a single byte from the underlying data.
		/// </summary>
		/// <returns>
		/// Returns an integer representation of the byte read (from
		/// 0 to 255) or -1 if it was impossible to read.
		/// </returns>
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
			ByteBuffer.WriteInt4(value, buffer, 0);
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

		/// <summary>
		/// Sets the length of the file.
		/// </summary>
		/// <param name="value">The new size of the file.</param>
		/// <remarks>
		/// If the given length is greater than the current size of the file,
		/// this will be grown, filling with empty information. Instead, if the 
		/// given length is smaller than the current length of the file, this
		/// will be truncated.
		/// </remarks>
		public abstract void SetLength(long value);

		/// <summary>
		/// Shifts all data after the given offset within the current
		/// container.
		/// </summary>
		/// <param name="offset">The zero-based offset from where to start
		/// shifting the data. A negative offset will cause the reduction of
		/// the size of the container. A value that exceeds the size of the
		/// container will cause an increase of the size.</param>
		/// <remarks>
		/// When calling this method, the current position of the pointer
		/// will not be changed.
		/// <para>
		/// The use of this method is intended when it is needed to insert or
		/// remove data before the end of the container.
		/// </para>
		/// </remarks>
		public abstract void Shift(long offset);

		/// <summary>
		/// Erase all data contained in the object.
		/// </summary>
		public abstract void Delete();

		/// <summary>
		/// Copies the contents of the current file, from the current position,
		/// to the given destination file, starting at its current position,
		/// for a given amount of bytes.
		/// </summary>
		/// <param name="destFile">The destination file where to copy the contents
		/// of the current one.</param>
		/// <param name="size">The amount of data, in bytes, to copy to the 
		/// destination file.</param>
		/// <remarks>
		/// If the current file contains less amount of data than the specified
		/// amount to copy, or the amount of data left after the current position
		/// is less than the given amount to copy, only the data available will be 
		/// copied to the destination file.
		/// <para>
		/// The destination file must not be the same file, source of the copy:
		/// this method cannot be used to copy data within the same file.
		/// </para>
		/// <para>
		/// The first aim of this function is to provide an efficient way of
		/// merging data between different <see cref="ITransaction">transactions</see>.
		/// </para>
		/// <para>
		/// When this method returns, the position location in both the source and
		/// target files will point to the end of the copied sequence.
		/// </para>
		/// </remarks>
		public abstract void CopyTo(DataFile destFile, long size);

		public abstract void ReplicateTo(DataFile target);
	}
}