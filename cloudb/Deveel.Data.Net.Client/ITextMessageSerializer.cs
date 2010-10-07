using System;

namespace Deveel.Data.Net.Client {
	public interface ITextMessageSerializer : IMessageSerializer {
		string ContentEncoding { get; }

		string ContentType { get; }
	}
}