using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public interface INodeSet : IEnumerable<INode> {
		void WriteTo(Stream output);
	}
}