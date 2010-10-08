using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	internal class ResponseMessageStream : ResponseMessage, IMessageStream {
		private readonly List<Message> messages;

		public ResponseMessageStream() {
			messages = new List<Message>();
		}

		public override MessageArguments Arguments {
			get { throw new NotSupportedException(); }
		}

		public override MessageAttributes Attributes {
			get { throw new NotSupportedException(); }
		}

		public override string Name {
			get { return base.Name; }
			set { throw new NotSupportedException(); }
		}

		public int MessageCount {
			get { return messages.Count; }
		}

		public MessageType Type {
			get { return MessageType; }
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