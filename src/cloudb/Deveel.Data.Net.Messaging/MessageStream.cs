using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Messaging {
	public sealed class MessageStream : IEnumerable<Message> {
		private readonly List<Message> messages = new List<Message>();

		public int MessageCount {
			get { return messages.Count; }
		}

		public Message GetMessage(int index) {
			return messages[index];
		}

		public void AddMessage(Message message) {
			messages.Add(message);
		}

		public IEnumerator<Message> GetEnumerator() {
			return messages.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}