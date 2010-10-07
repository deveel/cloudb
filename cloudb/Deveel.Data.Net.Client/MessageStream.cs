using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	internal class MessageStream : Message, IEnumerable<Message> {
		private readonly MessageType type;
		private readonly List<Message> messages;

		public MessageStream(MessageType type)
			: base(null) {
			this.type = type;
			messages = new List<Message>();
		}

		public override string Name {
			get { return null; }
			set { throw new NotSupportedException();}
		}

		public override MessageType Type {
			get { return type; }
		}

		public override MessageArguments Arguments {
			get { throw new NotSupportedException(); }
		}

		public override MessageAttributes Attributes {
			get { throw new NotSupportedException(); }
		}

		public IEnumerator<Message> GetEnumerator() {
			return messages.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int MessageCount {
			get { return messages.Count; }
		}

		public void AddMessage(Message message) {
			if (message.Type != type)
				throw new ArgumentException("The message is not supported in this stream.");

			messages.Add(message);
		}

		public Message GetMessage(int index) {
			return messages[index];
		}
		
		public static bool TryProcess(IMessageProcessor processor, Message request, out Message response) {
			MessageStream requestStream = request as MessageStream;
			if (requestStream == null) {
				response = null;
				return false;
			}
			
			response = new MessageStream(MessageType.Response);
			foreach (Message streamMessage in requestStream) {
				((MessageStream)response).AddMessage(processor.Process(streamMessage));
			}
			
			return true;
		}
	}
}