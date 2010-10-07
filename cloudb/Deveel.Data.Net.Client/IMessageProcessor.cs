using System;

namespace Deveel.Data.Net.Client {
	public interface IMessageProcessor {
		ResponseMessage Process(RequestMessage message);
	}
}