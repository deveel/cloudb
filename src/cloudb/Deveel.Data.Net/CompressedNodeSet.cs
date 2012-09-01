using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace Deveel.Data.Net {
	public sealed class CompressedNodeSet : NodeSet {
		internal CompressedNodeSet(NodeId[] nodeIds, byte[] buffer)
			: base(nodeIds, buffer) {
		}

		public override IEnumerator<Node> GetEnumerator() {
			return new CompressedNodeEnumerator(this);
		}

		#region CompressedNodeEnumerator

		private class CompressedNodeEnumerator : IEnumerator<Node> {
			public CompressedNodeEnumerator(CompressedNodeSet nodeSet) {
				this.nodeSet = nodeSet;
				compressedInput = new DeflateStream(new MemoryStream(nodeSet.Buffer), CompressionMode.Decompress);
			}

			private int index = -1;
			private readonly CompressedNodeSet nodeSet;
			private readonly DeflateStream compressedInput;

			#region Implementation of IDisposable

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				return ++index < nodeSet.NodeIds.Length;
			}

			public void Reset() {
				index = -1;
			}

			public Node Current {
				get { return new Node(nodeSet.NodeIds[index], compressedInput); }
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion
		}

		#endregion
	}
}