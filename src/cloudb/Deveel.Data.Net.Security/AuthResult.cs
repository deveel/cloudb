using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthResult {
		private readonly int code;
		private readonly IDictionary<string, object> authData;
		private readonly string message;
		private readonly IDictionary<string, object> outputData;
		private readonly object context;

		private const int SuccessCode = (int) AuthenticationCode.Success;

		public AuthResult(object context, int code, string message, IDictionary<string, object> authData) {
			this.context = context;
			this.code = code;
			this.message = message;
			this.authData = authData;

			outputData = new Dictionary<string, object>();
		}

		public AuthResult(object context, AuthenticationCode code, string message, IDictionary<string, object> authData)
			: this(context, (int)code, message, authData) {
		}

		public AuthResult(object context, int code, IDictionary<string, object> authData)
			: this(context, code, null, authData) {
		}

		public AuthResult(object context, AuthenticationCode code, string message)
			: this(context, (int)code, message) {
		}

		public AuthResult(object context, int code, string message)
			: this(context, code, message, null) {
		}

		public AuthResult(object context, AuthenticationCode code)
			: this(context, (int)code) {
		}

		public AuthResult(object context, int code)
			: this(context, code, (string)null) {
		}

		public object Context {
			get { return context; }
		}

		public string Message {
			get { return message; }
		}

		public IDictionary<string, object> AuthData {
			get { return authData; }
		}

		public int Code {
			get { return code; }
		}

		public bool Success {
			get { return code == SuccessCode; }
		}

		public IDictionary<string, object> OutputData {
			get { return outputData; }
		}
	}
}