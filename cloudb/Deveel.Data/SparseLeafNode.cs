using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	class SparseLeafNode : TreeLeaf {
		private readonly byte sparseByte;
		
		private readonly long node_ref;
		private readonly int leaf_size;
		
		public SparseLeafNode(long node_ref, byte sparce_byte, int leaf_size)
			: base() {
			this.node_ref = node_ref;
			this.leaf_size = leaf_size;
			this.sparseByte = sparce_byte;
		}
		
		public override long Id {
			get { return node_ref; }
		}
		
		public override int Length {
			get { return leaf_size; }
		}
		
		public override int Capacity {
			get { throw new InvalidOperationException(); }
		}
		
		public override long MemoryAmount {
			get { return 1 + 8 + 4 + 96; }
		}
		
		public override void Read(int position, byte[] buffer, int offset, int count) {
			int end = offset + count;
			for (int i = offset; i < end; ++i) {
				buffer[i] = sparseByte;
			}
		}
		
		public override void WriteTo(IAreaWriter area) {
			int sz = Length;
			for (int i = 0; i < sz; ++i) {
				area.WriteByte(sparseByte);
			}
		}
		
		public override void Shift(int position, int offset) {
			throw new InvalidOperationException();
		}
		
		public override void Write(int position, byte[] buffer, int offset, int count) {
			throw new InvalidOperationException();
		}
		
		public override void SetLength(int value) {
			throw new InvalidOperationException();
		}
		
		public static ITreeNode Create(long nodeId) {
			// static nodes are encoded into the reference
			int cmd = (int) nodeId;
			// Get the command
			int c = (int) (((long)cmd) & 0x0FF000000);
			if (c != 0x01000000)
				throw new ApplicationException("Unknown special static node.");
			
			// This is a sparse command
			// Get the byte which is the sparse array
			byte b = (byte) ((cmd & 0x0FF0000) >> 16);
			// Get the size value
			int sparseSize = (cmd & 0x0FFFF);
			// Create the sparse node
			return new SparseLeafNode(nodeId, b, sparseSize);
		}	
	}
}