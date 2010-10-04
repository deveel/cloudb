using System;

namespace Deveel.Data.Net {
	public interface ITextMethodSerializer : IMethodSerializer {
		string ContentEncoding { get; }

		string ContentType { get; }
	}
}