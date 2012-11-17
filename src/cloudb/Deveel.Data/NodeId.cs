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

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class NodeId : Quadruple {

		private static readonly long InMemoryHigh;
		private static readonly long SparseHigh;

		public NodeId(long[] components)
			: base(components) {
		}

		public NodeId(long high, long low)
			: base(high, low) {
		}

		static NodeId() {
			long code = 1;
			code = code << 60;
			InMemoryHigh = code;

			code = 2;
			code = code << 60;
			SparseHigh = code;
		}

		/// <summary>
		/// Returns the 4-bit encoded top part of the reference.
		/// </summary>
		public int Reserved {
			get { return ((int) (High >> 60)) & 0x0F; }
		}

		public bool IsInMemory {
			get { return Reserved == 1; }
		}

		public bool IsSpecial {
			get {
				int reserved = Reserved;
				return (reserved >= 2 && reserved < 8);
			}
		}

		public ITreeNode CreateSpecialTreeNode() {
			long c = (long)((ulong)High & 0x0F000000000000000L);
			// If it's a sparce special node,
			if (c == SparseHigh) {
				// Create the sparse node
				byte b = (byte) (High & 0x0FF);
				long sparseSize = Low;

				if (sparseSize > Int32.MaxValue || sparseSize < 0) {
					throw new ApplicationException("sparse_size out of range");
				}

				return new SparseLeafNode(this, b, (int) sparseSize);
			}

			throw new ApplicationException("Unknown special node.");
		}

		public static NodeId CreateInMemoryNode(long reference) {
			return new NodeId(InMemoryHigh, reference);
		}

		public static NodeId CreateSpecialSparseNode(byte value, long maxSize) {
			// Sanity check,
			if (maxSize < 0 || maxSize > Int32.MaxValue)
				throw new ApplicationException("Sparse node size out of range (" + maxSize + ")");

			return new NodeId((SparseHigh | value), maxSize);
		}

		private class SparseLeafNode : TreeLeaf {
			private readonly byte sparseByte;

			private readonly NodeId nodeId;
			private readonly int leafSize;

			public SparseLeafNode(NodeId nodeId, byte sparseByte, int leafSize) {
				this.nodeId = nodeId;
				this.leafSize = leafSize;
				this.sparseByte = sparseByte;
			}

			public override int Length {
				get { return leafSize; }
			}

			public override int Capacity {
				get { throw new ApplicationException("Static node does not have a meaningful capacity."); }
			}

			public override NodeId Id {
				get { return nodeId; }
			}

			public override long MemoryAmount {
				get {
					// The size of the member variables +96 byte estimate for heap use for
					// object maintenance.
					return 1 + 8 + 4 + 96;
				}
			}

			public override void SetLength(int value) {
				throw new NotSupportedException();
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				int end = offset + count;
				for (int i = offset; i < end; ++i) {
					buffer[i] = sparseByte;
				}
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override void WriteTo(IAreaWriter area) {
				int sz = Length;
				for (int i = 0; i < sz; ++i) {
					area.WriteByte(sparseByte);
				}
			}

			public override void Shift(int position, int offset) {
				throw new NotSupportedException();
			}
		}

		public static NodeId Parse(string s) {
			if (s == null)
				throw new ArgumentNullException("s");

			// Find the deliminator,
			int p = s.IndexOf(".", StringComparison.Ordinal);
			if (p == -1) {
				throw new FormatException();
			}

			long highv = Convert.ToInt64(s.Substring(0, p), 16);
			long lowv = Convert.ToInt64(s.Substring(p + 1), 16);
			return new NodeId(highv, lowv);
		}
	}
}