using System;
using System.IO;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data {
	public class NodeId : Quadruple {
		public NodeId(long high, long low)
			: base(high, low) {
		}

		public NodeId(long[] refs)
			: base(refs) {
		}

		static NodeId() {
			long code = 1;
			code = code << 60;
			InMemoryHigh = code;

			code = 2;
			code = code << 60;
			SparseHigh = code;
		}

		private static readonly long InMemoryHigh;
		private static readonly long SparseHigh;


		public int ReservedBits {
			get { return ((int)(High >> 60)) & 0x0F; }
		}

		public bool IsInMemory {
			get { return ReservedBits == 1; }
		}

		public bool IsSpecial {
			get {
				int res_bits = ReservedBits;
				return (res_bits >= 2 && res_bits < 8);
			}
		}

		public ITreeNode CreateSpecialTreeNode() {
			long c = (long)((ulong) High & 0x0F000000000000000L);
			// If it's a sparce special node,
			if (c == SparseHigh) {
				// Create the sparse node
				byte b = (byte)(High & 0x0FF);
				long sparse_size = Low;

				if (sparse_size > Int32.MaxValue || sparse_size < 0) {
					throw new ApplicationException("sparse_size out of range");
				}

				return new SparseLeafNode(this, b, (int)sparse_size);
			}
				
			throw new ApplicationException("Unknown special node.");
		}

		public static NodeId CreateInMemoryNode(long reference) {
			return new NodeId(InMemoryHigh, reference);
		}

		public static NodeId CreateSpecialSparseNode(byte b, long maxSize) {
			// Sanity check,
			if (maxSize < 0 || maxSize > Int32.MaxValue)
				throw new ArgumentOutOfRangeException("Sparse node size out of range (" + maxSize + ")");

			return new NodeId((SparseHigh | b), maxSize);
		}

		public static NodeId Parse(string s) {
			// Find the deliminator,
			int p = s.IndexOf(".");
			if (p == -1)
				throw new FormatException();

			long highv = Convert.ToInt64(s.Substring(0, p), 16);
			long lowv = Convert.ToInt64(s.Substring(p + 1), 16);
			return new NodeId(highv, lowv);
		}

		public override string ToString() {
			StringBuilder b = new StringBuilder();
			b.Append("0x" + High.ToString("X"));
			b.Append(".");
			b.Append("0x" + Low.ToString("X"));
			return b.ToString();
		}

		#region SparseLeafNode

		private sealed class SparseLeafNode : TreeLeaf {

			private readonly byte sparceByte;

			private readonly NodeId id;
			private readonly int length;

			public SparseLeafNode(NodeId id, byte sparceByte, int length) {
				this.id = id;
				this.length = length;
				this.sparceByte = sparceByte;
			}

			public override NodeId Id {
				get { return id; }
			}

			public override int Length {
				get { return length; }
			}

			public override int Capacity {
				get { throw new ApplicationException("Static node does not have a meaningful capacity."); }
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				int end = offset + count;
				for (int i = offset; i < end; ++i) {
					buffer[i] = sparceByte;
				}
			}

			public override void WriteTo(IAreaWriter area) {
				int sz = Length;
				for (int i = 0; i < sz; ++i) {
					area.WriteByte(sparceByte);
				}
			}

			public override void Shift(int position, int offset) {
				throw new IOException("Write methods not available for immutable store leaf.");
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				throw new IOException("Write methods not available for immutable store leaf.");
			}

			public override void SetLength(int value) {
				throw new IOException("Write methods not available for immutable store leaf.");
			}

			public override long MemoryAmount {
				get {
					// The size of the member variables +96 byte estimate for heap use for
					// object maintenance.
					return 1 + 8 + 4 + 96;
				}
			}
		}

		#endregion
	}
}