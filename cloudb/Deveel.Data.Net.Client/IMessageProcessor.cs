using System;

namespace Deveel.Data.Net.Client {
	public interface IMessageProcessor {
		Message ProcessMessage(Message message);
	}
}