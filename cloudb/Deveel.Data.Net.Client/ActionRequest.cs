using System;

namespace Deveel.Data.Net.Client {
	public sealed class ActionRequest : ICloneable, IAttributesHandler {
		private readonly RequestType type;
		private readonly IPathTransaction transaction;
		private ActionArguments arguments;
		private ActionResponse response;
		private ActionAttributes attributes;
		private bool readOnly;

		public const string ResourceIdName = "resource-id";
		public const string ItemIdName = "item-id";

		internal ActionRequest(RequestType type, IPathTransaction transaction) {
			this.type = type;
			this.transaction = transaction;
			arguments = new ActionArguments(false);
			attributes = new ActionAttributes(this);
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

		internal void Seal() {
			readOnly = true;
			arguments.Seal();
		}

		public object Clone() {
			ActionRequest request = new ActionRequest(type, transaction);
			request.arguments = (ActionArguments) arguments.Clone();
			request.attributes = (ActionAttributes)attributes.Clone();
			return request;
		}

		public ActionResponse CreateResponse() {
			if (response != null)
				throw new InvalidOperationException("A response was previously created.");

			response = new ActionResponse(this, transaction);
			return response;
		}
	}
}