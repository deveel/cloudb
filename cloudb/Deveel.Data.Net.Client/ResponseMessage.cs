using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	public class ResponseMessage : Message {
		private readonly RequestMessage request;
		private MessageResponseCode code;

		public ResponseMessage(string name, RequestMessage request)
			: base(name) {
			if (request != null)
				request.OnResponseMessageCreated(this);

			this.request = request;
		}

		public ResponseMessage(string name)
			: this(name, null) {
		}

		public ResponseMessage()
			: this(null) {
		}
		
		public ResponseMessage(RequestMessage request, ResponseMessage baseResponse)
			: this(baseResponse != null ? baseResponse.Name : null, request) {
			if (baseResponse == null)
				throw new ArgumentNullException("baseResponse");
			foreach(KeyValuePair<string, object> attribute in baseResponse.attributes)
				attributes.Add(attribute.Key, attribute.Value);
			foreach(MessageArgument argument in baseResponse.arguments)
				arguments.Add(argument);
		}

		public override MessageType MessageType {
			get { return MessageType.Response; }
		}

		public RequestMessage Request {
			get { return request; }
		}
		
		public MessageResponseCode Code {
			get {
				if (HasError && code == MessageResponseCode.Success)
					code = MessageResponseCode.Error;

				return code;
			}
			set { code = value; }
		}

		public bool HasReturnValue {
			get { return Arguments.Count == 1 && !HasError; }
		}

		public object ReturnValue {
			get { return Arguments.Count == 1 && !HasError ? Arguments[0].Value : null; }
		}
	}
}