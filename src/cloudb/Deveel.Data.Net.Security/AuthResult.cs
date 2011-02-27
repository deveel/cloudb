using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthResult {
		private readonly bool success;
		private readonly int code;
		private readonly IDictionary<string, object> authData;
		private readonly string message;

		public AuthResult(bool success, int code, string message, IDictionary<string, object> authData) {
			this.success = success;
			this.code = code;
			this.message = message;
			this.authData = authData;
		}

		public AuthResult(bool success, int code, IDictionary<string, object> authData)
			: this(success, code, null, authData) {
		}

		public AuthResult(bool success, int code, string message)
			: this(success, code, message, null) {
		}

		public AuthResult(bool success, int code)
			: this(success, code, (string)null) {
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
			get { return success; }
		}
	}
}