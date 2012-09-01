using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public abstract class NodeSet : IEnumerable<Node> {
		protected NodeSet(NodeId[] nodeIds, byte[] buffer) {
			this.nodeIds = nodeIds;
			this.buffer = buffer;
		}

		private readonly NodeId[] nodeIds;
		private readonly byte[] buffer;

		public NodeId [] NodeIds {
			get { return nodeIds; }
		}

		protected internal byte [] Buffer {
			get { return buffer; }
		}

		/*
		internal void WriteTo(Stream output) {
			BinaryWriter writer = new BinaryWriter(output);
			writer.Write(nodeIds.Length);
			for (int i = 0; i < nodeIds.Length; ++i) {
				writer.Write(nodeIds[i]);
			}

			writer.Write(buffer.Length);
			writer.Write(buffer);
		}
		*/

		#region Implementation of IEnumerable

		public abstract IEnumerator<Node> GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#endregion
	}
}