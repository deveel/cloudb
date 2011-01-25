using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class SingleNodeSet : NodeSet {
		internal SingleNodeSet(long blockId, int dataId, byte[] buffer)
			: base(new long[] { new DataAddress(blockId, dataId).Value}, buffer) {
		}

		internal SingleNodeSet(long nodeId, byte[] buffer)
			: base(new long[] { nodeId}, buffer) {
		}

		internal SingleNodeSet(long[] nodeIds, byte[] buffer)
			: base(nodeIds, buffer) {
		}

		public override IEnumerator<Node> GetEnumerator() {
			return new SimpleNodeEnumerator(this);
		}

		#region SimpleNodeEnumerator

		private class SimpleNodeEnumerator : IEnumerator<Node> {
			public SimpleNodeEnumerator(SingleNodeSet nodeSet) {
				this.nodeSet = nodeSet;
			}

			private readonly SingleNodeSet nodeSet;
			private int index = -1;

			#region Implementation of IDisposable

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				return ++index < 1;
			}

			public void Reset() {
				index = -1;
			}

			public Node Current {
				get { return new Node(nodeSet.NodeIds[0], nodeSet.Buffer); }
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion
		}

		#endregion
	}
}