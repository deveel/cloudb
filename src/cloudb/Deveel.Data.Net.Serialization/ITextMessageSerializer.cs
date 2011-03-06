using System;

namespace Deveel.Data.Net.Serialization {
	public interface ITextMessageSerializer : IMessageSerializer {
		string ContentEncoding { get; }

		string ContentType { get; }
	}
}