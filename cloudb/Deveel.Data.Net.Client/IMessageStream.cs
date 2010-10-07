using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	internal interface IMessageStream : IEnumerable<Message> {
		int MessageCount { get; }

		MessageType Type { get; }

		Message GetMessage(int index);

		void AddMessage(Message message);
	}
}