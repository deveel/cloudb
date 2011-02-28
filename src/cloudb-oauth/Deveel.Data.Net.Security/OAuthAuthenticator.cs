using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthAuthenticator : IAuthenticator {
		public void Init(ConfigSource config) {
		}

		public AuthResult Authenticate(AuthRequest authRequest) {
			throw new NotImplementedException();
		}
	}
}