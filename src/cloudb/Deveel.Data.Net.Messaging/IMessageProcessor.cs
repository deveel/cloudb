using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Messaging {
	public interface IMessageProcessor {
		IEnumerable<Message> Process(IEnumerable<Message> stream);
	}
}