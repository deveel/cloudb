//
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

namespace Deveel.Data {
	/// <summary>
	/// An interface to a representation of a flexible structure of binary data 
	/// contained within a database system.
	/// </summary>
	/// <remarks>
	/// The architecture of a <see cref="IDataFile"/> is flexible because it
	/// can be grown, shrunk, contained data can be moved, removed, inserted
	/// at arbitrary positions.
	/// <para>
	/// A <see cref="IDataFile"/> is constructed by pointing a portion of data
	/// present in a database, making this pointer to advance at every write
	/// operation, or by setting a current position different from the original one.
	/// </para>
	/// </remarks>
	public interface IDataFile {
		/// <summary>
		/// Gets the length of the portion of data pointed by this file.
		/// </summary>
		long Length { get; }

		/// <summary>
		/// Gets or sets the current position of the pointer.
		/// </summary>
		long Position { get; set; }


		/// <summary>
		/// Reads a sequence of bytes from the underlying stream and fills the
		/// given buffer for the amount specified starting at the offset indicated.
		/// </summary>
		/// <param name="buffer">The buffer to fill with the bytes read from the stream</param>
		/// <param name="offset">The starting offset within the given <paramref name="buffer"/>
		/// from where to start copy to.</param>
		/// <param name="count">The number of bytes to copy into the given buffer.</param>
		/// <returns>
		/// Returns the number of bytes effectivelly copied into the given buffer.
		/// </returns>
		int Read(byte[] buffer, int offset, int count);

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
		void SetLength(long value);

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
		void Shift(long offset);

		/// <summary>
		/// Erase all data contained in the object.
		/// </summary>
		void Delete();

		/// <summary>
		/// Writes the contents of the given <paramref name="buffer"/> from the current position
		/// of the underlying stream, starting from the given offset for the amount specified of
		/// the given buffer.
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="count"></param>
		/// <remarks>
		/// Any existing data in the file past the current position is overwritten by this operation 
		/// (up to the amount of data written).
		/// <para>
		/// If, during the write, the position extends past the end of the file, the size of the file 
		/// is increased to make room of the data being written.
		/// </para>
		/// </remarks>
		void Write(byte[] buffer, int offset, int count);

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
		/// <para>
		/// <b>Note:</b> This is a legacy code. Use <see cref="CopyFrom"/> instead.
		/// </para>
		/// <para>
		/// If the current file contains less amount of data than the specified
		/// amount to copy, or the amount of data left after the current position
		/// is less than the given amount to copy, only the data available will be 
		/// copied to the destination file.
		/// </para>
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
		/// <seealso cref="CopyFrom"/>
		void CopyTo(IDataFile destFile, long size);

		/// <summary>
		/// Copies the contents of the given file, to this file, starting at its current 
		/// position, for a given amount of bytes.
		/// </summary>
		/// <param name="sourceFile"></param>
		/// <param name="size"></param>
		void CopyFrom(IDataFile sourceFile, long size);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="destFile"></param>
		void ReplicateTo(IDataFile destFile);

		/// <summary>
		/// Replaces the entire contents of this file with the content of the given file.
		/// </summary>
		/// <param name="sourceFile"></param>
		void ReplicateFrom(IDataFile sourceFile);
	}
}