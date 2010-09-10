using System;

namespace Deveel.Data.Net {
	public interface IBlockStore {
		bool Open();
		
		void Write(int dataId, byte[] buffer, int offset, int length);
		
		NodeSet GetNodeSet(int dataId);
		
		void Delete(int dataId);
		
		void Flush();
		
		void Close();
		
		long CreateChecksum();
	}
}