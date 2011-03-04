using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class SingleNodeSet : NodeSet {
		internal SingleNodeSet(BlockId blockId, int dataId, byte[] buffer)
			: base(new NodeId[] { new DataAddress(blockId, dataId).Value}, buffer) {
		}

		internal SingleNodeSet(NodeId nodeId, byte[] buffer)
			: base(new NodeId[] { nodeId}, buffer) {
		}

		internal SingleNodeSet(NodeId[] nodeIds, byte[] buffer)
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