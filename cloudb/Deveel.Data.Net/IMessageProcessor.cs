using System;

namespace Deveel.Data.Net {
	public interface IMessageProcessor {
		MessageStream Process(MessageStream messageStream);
	}
}