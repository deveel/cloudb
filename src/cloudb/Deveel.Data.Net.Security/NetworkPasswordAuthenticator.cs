using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public sealed class NetworkPasswordAuthenticator : IAuthenticator {
		private string password;

		public NetworkPasswordAuthenticator(string password) {
			if (password == null) 
				throw new ArgumentNullException("password");

			this.password = password;
		}

		private NetworkPasswordAuthenticator() {
		}

		public string Password {
			get { return password; }
		}

		public string Mechanism {
			get { return "netpass"; }
		}

		void IAuthenticator.Init(ConfigSource config) {
			password = config.GetString("password");
		}

		public void CollectData(IDictionary<string, AuthObject> authData) {
			authData["password"] = new AuthObject(AuthDataType.String, password);
		}

		public AuthResult Authenticate(AuthRequest authRequest) {
			string pass;
			if (!authRequest.AuthData.TryGetValue("password", out pass))
				return new AuthResult(authRequest.Context, AuthenticationCode.MissingData);

		}

		void IAuthenticator.EndContext(object context) {
		}
	}
}