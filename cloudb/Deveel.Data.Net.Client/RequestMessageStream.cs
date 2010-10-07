using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	internal class RequestMessageStream : RequestMessage, IMessageStream {
		private readonly List<Message> messages;

		public RequestMessageStream() {
			messages = new List<Message>();
		}

		public int MessageCount {
			get { return messages.Count; }
		}

		public MessageType Type {
			get { return MessageType; }
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

		public static bool TryProcess(IMessageProcessor processor, RequestMessage request, out ResponseMessage response) {
			RequestMessageStream requestStream = request as RequestMessageStream;
			if (requestStream == null) {
				response = null;
				return false;
			}

			response = new ResponseMessageStream();
			foreach (RequestMessage streamMessage in requestStream) {
				((ResponseMessageStream)response).AddMessage(processor.Process(streamMessage));
			}

			return true;
		}
	}
}