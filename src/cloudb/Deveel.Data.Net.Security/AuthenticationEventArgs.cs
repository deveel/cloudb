using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public delegate void AuthenticationEventHandler(object sender, AuthenticationEventArgs e);

	public sealed class AuthenticationEventArgs : EventArgs {
		private readonly int code;
		private readonly AuthenticationException error;
		private readonly bool hasError;
		private readonly string message;
		private readonly IDictionary<string, AuthObject> authData;

		internal AuthenticationEventArgs(int code, string message, IDictionary<string,AuthObject> authData) {
			this.code = code;
			this.message = message;
			this.authData = new Dictionary<string, AuthObject>(authData);
		}

		internal AuthenticationEventArgs(AuthenticationException error, IDictionary<string,AuthObject> authData)
			: this(error.Code, error.Message, authData) {
			this.error = error;
			hasError = true;
		}

		public IDictionary<string, AuthObject> AuthData {
			get { return authData; }
		}

		public string Message {
			get { return message; }
		}

		public bool HasError {
			get { return hasError; }
		}

		public AuthenticationException Error {
			get { return error; }
		}

		public int Code {
			get { return code; }
		}
	}
}