using System;

namespace Deveel.Data.Net.Client {
	public class RequestMessage : Message, ICloneable {
		private ResponseMessage response;

		public const string ResourceIdName = "resource-id";
		public const string ItemIdName = "item-id";

		public RequestMessage(string name)
			: base(name) {
		}

		public RequestMessage()
			: this(null) {
		}

		public override MessageType MessageType {
			get { return MessageType.Request; }
		}

		public bool HasResourceId {
			get { return Attributes.Contains(ResourceIdName); }
		}

		public bool HasItemId {
			get { return Attributes.Contains(ItemIdName); }
		}

		public object ResourceId {
			get {  return Attributes[ResourceIdName]; }
			set { Attributes[ResourceIdName] = value; }
		}

		public object ItemId {
			get { return Attributes[ItemIdName]; }
			set { Attributes[ItemIdName] = value; }
		}

		public bool HasResponse {
			get { return response != null; }
		}

		public ResponseMessage Response {
			get { return response; }
		}

		internal void OnResponseMessageCreated(ResponseMessage message) {
			if (response != null)
				throw new InvalidOperationException("A response for this request was already created.");

			response = message;
		}

		internal virtual RequestMessage CreateClone() {
			return new RequestMessage();
		}

		public virtual object Clone() {
			RequestMessage request = CreateClone();
			request.Name = Name;
			request.arguments = (MessageArguments) Arguments.Clone();
			request.attributes = (MessageAttributes)Attributes.Clone();
			return request;
		}

		public ResponseMessage CreateResponse() {
			return CreateResponse(null);
		}

		public ResponseMessage CreateResponse(string responseName) {
			return new ResponseMessage(responseName, this);
		}
	}
}