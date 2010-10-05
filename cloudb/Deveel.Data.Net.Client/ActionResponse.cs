using System;

namespace Deveel.Data.Net.Client {
	public sealed class ActionResponse {
		private readonly ActionRequest request;
		private readonly IPathTransaction transaction;
		private readonly ActionArguments arguments;
		private ActionResponseCode code;

		internal ActionResponse(ActionRequest request, IPathTransaction transaction) {
			arguments = new ActionArguments(false);
			this.transaction = transaction;
			this.request = request;
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
	}
}