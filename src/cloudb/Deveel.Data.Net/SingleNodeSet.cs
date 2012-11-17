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