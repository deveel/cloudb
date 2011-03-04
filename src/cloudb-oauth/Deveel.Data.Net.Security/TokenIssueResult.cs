using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public sealed class TokenIssueResult {
		private readonly bool success;
		private string message;
		private string problemType;
		private readonly NameValueCollection parameters;

		public TokenIssueResult(bool success, string message, string problemType) {
			this.success = success;
			this.problemType = problemType;
			this.message = message;
			parameters = new NameValueCollection();
		}

		public TokenIssueResult(bool success, string message)
			: this(success, message, null) {
		}

		public TokenIssueResult(bool success)
			: this(success, null) {
		}

		public NameValueCollection Parameters {
			get { return parameters; }
		}

		public string ProblemType {
			get { return problemType; }
			set { problemType = value; }
		}

		public string Message {
			get { return message; }
			set { message = value; }
		}

		public bool Success {
			get { return success; }
		}
	}
}