using System;

namespace Deveel.Data.Net.Security {
	public sealed class AuthRequest : AuthMessage {
		private AuthResponse response;

		public AuthRequest(object context, string mechanism) 
			: base(context, mechanism) {
		}

		public AuthResponse Respond(int code) {
			if (response != null)
				throw new InvalidOperationException("A response to the request already created.");

			response = new AuthResponse(Context, Mechanism, code, Arguments);
			return response;
		}

		public AuthResponse Respond(AuthenticationCode code) {
			return Respond((int) code);
		}
	}
}