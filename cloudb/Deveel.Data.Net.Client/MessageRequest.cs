using System;

namespace Deveel.Data.Net.Client {
	public class MessageRequest : Message, ICloneable {
		private MessageResponse response;

		public const string ResourceIdName = "resource-id";
		public const string ItemIdName = "item-id";

		public MessageRequest(string name)
			: base(name) {
		}

		public MessageRequest()
			: this(null) {
		}

		public override MessageType Type {
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

		public MessageResponse Response {
			get { return response; }
		}

		internal virtual MessageRequest CreateClone() {
			return new MessageRequest();
		}

		public virtual object Clone() {
			MessageRequest request = CreateClone();
			request.Name = Name;
			request.arguments = (MessageArguments) Arguments.Clone();
			request.attributes = (MessageAttributes)Attributes.Clone();
			return request;
		}

		public MessageResponse CreateResponse() {
			return CreateResponse(Name);
		}

		public MessageResponse CreateResponse(string responseName) {
			response = new MessageResponse(responseName, this);
			return response;
		}
	}
}