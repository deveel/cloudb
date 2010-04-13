using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public interface IMessageProcessor {
		List<Message> Process(List<Message> messages);
	}
}