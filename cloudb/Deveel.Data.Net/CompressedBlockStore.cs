using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class CompressedBlockStore : IBlockStore {
		private readonly long blockId;
		private readonly string fileName;
		
		public CompressedBlockStore(long blockId, string fileName) {
			this.blockId = blockId;
			this.fileName = fileName;
		}
		
		public bool Exists {
			get {
				throw new NotImplementedException();
			}
		}
		
		public int Type {
			get { return 2; }
		}
		
		public bool Open()
		{
			throw new NotImplementedException();
		}
		
		public void Write(int dataId, byte[] buffer, int offset, int length)
		{
			throw new NotImplementedException();
		}
		
		public int Read(int dataId, byte[] buffer, int offset, int length)
		{
			throw new NotImplementedException();
		}
		
		public Stream OpenInputStream()
		{
			throw new NotImplementedException();
		}
		
		public NodeSet GetNodeSet(int dataId)
		{
			throw new NotImplementedException();
		}
		
		public void Delete(int dataId)
		{
			throw new NotImplementedException();
		}
		
		public void Flush()
		{
			throw new NotImplementedException();
		}
		
		public void Close()
		{
			throw new NotImplementedException();
		}
		
		public long CreateChecksum()
		{
			throw new NotImplementedException();
		}
		
		public static void Compress(string sourceFile, string destFile) {
			throw new NotImplementedException();
		}
	}
}