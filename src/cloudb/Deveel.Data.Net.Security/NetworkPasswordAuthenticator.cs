using System;

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

		public AuthResult Authenticate(AuthenticationPoint authPoint, AuthRequest authRequest) {
			throw new NotImplementedException();
		}
	}
}