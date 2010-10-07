using System;

namespace Deveel.Data.Net.Client {
	public interface ITextActionSerializer : IActionSerializer {
		string ContentEncoding { get; }

		string ContentType { get; }
	}
}