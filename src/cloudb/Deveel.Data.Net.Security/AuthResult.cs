using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthResult {
		private readonly bool success;
		private readonly int code;
		private readonly bool hasError;
		private readonly IDictionary<string, object> authData;
		private readonly string message;
		private readonly IDictionary<string, object> outputData;

		public AuthResult(bool success, int code, string message, IDictionary<string, object> authData) {
			this.success = success;
			this.code = code;
			this.message = message;
			this.authData = authData;
			outputData = new Dictionary<string, object>();
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

		public AuthResult(bool success)
			: this(success, -1) {
			hasError = false;
		}

		public bool HasError {
			get { return hasError; }
		}

		public IDictionary<string, object> OutputData {
			get { return outputData; }
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