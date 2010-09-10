using System;
using System.IO;

namespace Deveel.Data.Net {
	public interface IBlockStore {
		bool Exists { get; }
		
		int Type { get; }
		
		
		bool Open();
		
		void Write(int dataId, byte[] buffer, int offset, int length);
		
		int Read(int dataId, byte[] buffer, int offset, int length);
		
		Stream OpenInputStream();
		
		NodeSet GetNodeSet(int dataId);
		
		void Delete(int dataId);
		
		void Flush();
		
		void Close();
		
		long CreateChecksum();
	}
}