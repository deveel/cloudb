using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class Node {
		private readonly long id;
		private readonly Stream input;

		public Node(long id, Stream input) {
			this.id = id;
			this.input = input;
		}

		public Node(long id, byte[] input)
			: this(id, new MemoryStream(input)) {
		}

		public long Id {
			get { return id; }
		}

		public Stream Input {
			get { return input; }
		}
	}
}