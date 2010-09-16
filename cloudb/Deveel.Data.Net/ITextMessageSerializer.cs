using System;

namespace Deveel.Data.Net {
	public interface ITextMessageSerializer : IMessageSerializer {
		string ContentEncoding { get; }
		
		string ContentType { get; }
	}
}