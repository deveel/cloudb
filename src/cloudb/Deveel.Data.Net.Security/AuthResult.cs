using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthResult {
		private readonly int code;
		private readonly IDictionary<string, object> authData;
		private readonly string message;
		private readonly IDictionary<string, object> outputData;

		private const int SuccessCode = (int) AuthenticationCode.Success;

		public AuthResult(int code, string message, IDictionary<string, object> authData) {
			this.code = code;
			this.message = message;
			this.authData = authData;

			outputData = new Dictionary<string, object>();
		}

		public AuthResult(AuthenticationCode code, string message, IDictionary<string, object> authData)
			: this((int)code, message, authData) {
		}

		public AuthResult(int code, IDictionary<string, object> authData)
			: this(code, null, authData) {
		}

		public AuthResult(AuthenticationCode code, string message)
			: this((int)code, message) {
		}

		public AuthResult(int code, string message)
			: this(code, message, null) {
		}

		public AuthResult(AuthenticationCode code)
			: this((int)code) {
		}

		public AuthResult(int code)
			: this(code, (string)null) {
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