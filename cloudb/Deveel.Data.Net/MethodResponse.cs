using System;

namespace Deveel.Data.Net {
	public sealed class MethodResponse {
		private readonly MethodRequest request;
		private readonly IPathTransaction transaction;
		private readonly ArgumentList arguments;
		private MethodResponseCode code;

		internal MethodResponse(MethodRequest request, IPathTransaction transaction) {
			arguments = new ArgumentList(false);
			this.transaction = transaction;
			this.request = request;
		}

		public IPathTransaction Transaction {
			get { return transaction; }
		}

		public ArgumentList Arguments {
			get { return arguments; }
		}

		public MethodRequest Request {
			get { return request; }
		}
		
		public MethodResponseCode Code {
			get { return code; }
			set { code = value; }
		}
	}
}