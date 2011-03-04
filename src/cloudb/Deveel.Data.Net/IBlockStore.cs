using System;
using System.IO;

namespace Deveel.Data.Net {
	/// <summary>
	/// An interface used to access and modify data stored
	/// in a block store component.
	/// </summary>
	public interface IBlockStore {
		/// <summary>
		/// Verifies if the block storage exists within the
		/// storage context.
		/// </summary>
		bool Exists { get; }
		
		/// <summary>
		/// Gets a code that idenitfies the kind of block
		/// storage of the instance.
		/// </summary>
		/// <remarks>
		/// Since the architecture of the system is open, it is
		/// required to provide a code that uniquely identifies
		/// the kind of data storage handled, to permit to the
		/// communication protocol to correctly identify the
		/// format of the destination/origin of data to be transported.
		/// </remarks>
		int Type { get; }
		
		
		/// <summary>
		/// Opens the block store.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the store has been created,
		/// or <b>false</b> if otherwise it was opened an existing
		/// block store.
		/// </returns>
		bool Open();
		
		/// <summary>
		/// Writes the given data in the block pointer given
		/// </summary>
		/// <param name="dataId">The identifier of the portion of data, that must
		/// be ranging between 0 and 16383 (the maximum number of nodes that can be
		/// stored in a block).</param>
		/// <param name="buffer">The data to write</param>
		/// <param name="offset">The offset within the data buffer from where
		/// to start writing.</param>
		/// <param name="length">The amount of data to write (65535 bytes maximum).</param>
		void Write(int dataId, byte[] buffer, int offset, int length);
		
		/// <summary>
		/// Reads an amount of data given from the block at the given pointer.
		/// </summary>
		/// <param name="dataId">The identifier of the portion of data from where to 
		/// start reading, </param>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		/// <returns></returns>
		int Read(int dataId, byte[] buffer, int offset, int length);
		
		Stream OpenInputStream();
		
		NodeSet GetNodeSet(int dataId);
		
		void Delete(int dataId);
		
		void Flush();
		
		void Close();
		
		long CreateChecksum();
	}
}