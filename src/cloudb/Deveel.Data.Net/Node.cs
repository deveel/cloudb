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