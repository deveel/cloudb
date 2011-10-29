using System;

namespace Deveel.Data.Net.Security {
	public sealed class AuthResponse : AuthMessage {
		private readonly int code;

		internal AuthResponse(object context, string mechanism, int code, AuthMessageArguments arguments)
			: base(context, mechanism) {
			this.code = code;

			foreach (AuthMessageArgument argument in arguments) {
				arguments.Add(argument.Name, argument.Value);
			}
		}

		public int Code {
			get { return code; }
		}

		public bool Success {
			get { return code == (int)AuthenticationCode.Success; }
		}
	}
}