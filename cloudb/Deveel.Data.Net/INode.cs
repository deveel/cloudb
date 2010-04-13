using System;
using System.IO;

namespace Deveel.Data.Net {
	public interface INode {
		long Id { get; }

		Stream Input { get;  }
	}
}