using System;

namespace Deveel.Data.Net.Client {
	public sealed class ActionRequest : ICloneable, IAttributesHandler {
		private readonly string clientType;
		private readonly RequestType type;
		private readonly IPathTransaction transaction;
		private ActionArguments arguments;
		private ActionResponse response;
		private ActionAttributes attributes;
		private bool readOnly;

		public const string ResourceIdName = "resource-id";
		public const string ItemIdName = "item-id";

		internal ActionRequest(string clientType, RequestType type, IPathTransaction transaction) {
			this.clientType = clientType;
			this.type = type;
			this.transaction = transaction;
			arguments = new ActionArguments(false);
			attributes = new ActionAttributes(this);
		}

		public string ClientType {
			get { return clientType; }
		}

		public bool IsRestClient {
			get { return String.Compare(clientType, "rest", true) == 0; }
		}

		public bool IsRpcClient {
			get { return String.Compare(clientType, "rpc", true) == 0; }
		}

		public RequestType Type {
			get { return type; }
		}

		public IPathTransaction Transaction {
			get { return transaction; }
		}

		public ActionArguments Arguments {
			get { return arguments; }
		}

		public ActionAttributes Attributes {
			get { return attributes; }
		}

		bool IAttributesHandler.IsReadOnly {
			get { return readOnly; }
		}

		public bool HasResourceId {
			get { return attributes.Contains(ResourceIdName); }
		}

		public bool HasItemId {
			get { return attributes.Contains(ItemIdName); }
		}

		public object ResourceId {
			get {  return attributes[ResourceIdName]; }
			set { attributes[ResourceIdName] = value; }
		}

		public object ItemId {
			get { return attributes[ItemIdName]; }
			set { attributes[ItemIdName] = value; }
		}

		public bool HasResponse {
			get { return response != null; }
		}

		public ActionResponse Response {
			get { return response; }
		}

		internal void Seal() {
			readOnly = true;
			arguments.Seal();
		}

		public object Clone() {
			ActionRequest request = new ActionRequest(clientType, type, transaction);
			request.arguments = (ActionArguments) arguments.Clone();
			request.attributes = (ActionAttributes)attributes.Clone();
			return request;
		}

		public ActionResponse CreateResponse() {
			return CreateResponse(null);
		}

		public ActionResponse CreateResponse(string name) {
			response = new ActionResponse(name, this, transaction);
			return response;
		}
	}
}