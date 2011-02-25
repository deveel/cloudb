using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class Node {
		private readonly NodeId id;
		private readonly Stream input;

		public Node(NodeId id, Stream input) {
			this.id = id;
			this.input = input;
		}

		public Node(NodeId id, byte[] input)
			: this(id, new MemoryStream(input)) {
		}

		public NodeId Id {
			get { return id; }
		}

		public Stream Input {
			get { return input; }
		}
	}
}