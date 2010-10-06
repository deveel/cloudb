using System;

namespace Deveel.Data.Net.Client {
	public sealed class MessageResponse : Message {
		private readonly MessageRequest request;
		private MessageResponseCode code;

		internal MessageResponse(string name, MessageRequest request)
			: base(name) {
			this.request = request;
		}

		public override MessageType Type {
			get { return MessageType.Response; }
		}

		public MessageRequest Request {
			get { return request; }
		}
		
		public MessageResponseCode Code {
			get { return code; }
			set { code = value; }
		}
	}
}