using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	public sealed class MessageStream : Message, IEnumerable<Message> {
		private readonly List<Message> messages;
		private readonly MessageType type;

		internal MessageStream(MessageType type)
			: base(null) {
			this.type = type;
			messages = new List<Message>();
		}

		public override MessageType MessageType {
			get { return type; }
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

		public static MessageStream NewRequest() {
			return new MessageStream(MessageType.Request);
		}

		public static MessageStream NewResponse() {
			return new MessageStream(MessageType.Response);
		}

		public static bool TryProcess(IMessageProcessor processor, Message request, out Message response) {
			MessageStream requestStream = request as MessageStream;
			if (requestStream == null) {
				response = null;
				return false;
			}

			response = new MessageStream(MessageType.Response);
			foreach (RequestMessage streamMessage in requestStream) {
				((MessageStream)response).AddMessage(processor.Process(streamMessage));
			}

			return true;
		}
	}
}