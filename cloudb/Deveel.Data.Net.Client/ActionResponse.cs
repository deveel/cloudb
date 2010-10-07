using System;

namespace Deveel.Data.Net.Client {
	public sealed class ActionResponse : IAttributesHandler {
		private readonly string name;
		private readonly ActionRequest request;
		private readonly IPathTransaction transaction;
		private readonly ActionArguments arguments;
		private readonly ActionAttributes attributes;
		private ActionResponseCode code;

		internal ActionResponse(string name, ActionRequest request, IPathTransaction transaction) {
			this.name = name;
			arguments = new ActionArguments(false);
			attributes = new ActionAttributes(this);
			this.transaction = transaction;
			this.request = request;
		}

		public bool HasName {
			get { return !String.IsNullOrEmpty(name); }
		}

		public string Name {
			get { return name; }
		}

		public IPathTransaction Transaction {
			get { return transaction; }
		}

		public ActionArguments Arguments {
			get { return arguments; }
		}

		public ActionRequest Request {
			get { return request; }
		}
		
		public ActionResponseCode Code {
			get { return code; }
			set { code = value; }
		}

		bool IAttributesHandler.IsReadOnly {
			get { return false; }
		}

		public ActionAttributes Attributes {
			get { return attributes; }
		}
	}
}