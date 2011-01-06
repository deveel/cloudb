using System;

namespace Deveel.Data.Net.Client {
	public interface IMessageProcessor {
		Message Process(Message message);
	}
}