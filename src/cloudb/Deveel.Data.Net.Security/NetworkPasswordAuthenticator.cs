using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public sealed class NetworkPasswordAuthenticator : IServiceAuthenticator {
		private string password;

		public NetworkPasswordAuthenticator(string password) {
			if (password == null) 
				throw new ArgumentNullException("password");

			this.password = password;
		}

		public NetworkPasswordAuthenticator() {
		}

		public string Password {
			get { return password; }
			set { password = value; }
		}

		public string Mechanism {
			get { return "netpass"; }
		}

		AuthResponse IAuthenticator.Authenticate(AuthRequest authRequest) {
			AuthMessageArgument pass = authRequest.Arguments["password"];
			if (pass == null)
				return authRequest.Respond(AuthenticationCode.MissingData);

			AuthObject passValue = pass.Value;
			if (passValue == null)
				return authRequest.Respond(AuthenticationCode.MissingData);

			string s = passValue.Value as string;
			if (String.IsNullOrEmpty(s))
				return authRequest.Respond(AuthenticationCode.MissingData);

			if (!String.Equals(s, password, StringComparison.InvariantCulture))
				return authRequest.Respond(AuthenticationCode.InvalidAuthentication);

			return authRequest.Respond(AuthenticationCode.Success);
		}

		void IAuthenticator.EndContext(object context) {
		}

		public AuthRequest CreateRequest(AuthResponse authResponse) {
			AuthRequest request = new AuthRequest(null, Mechanism);
			request.Arguments.Add("password", password);
			return request;
		}

		void IAuthenticator.Init(ConfigSource config) {
			password = config.GetString("password");
		}
	}
}