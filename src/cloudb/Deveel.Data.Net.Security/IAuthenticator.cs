using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface IAuthenticator {
		void Init(ConfigSource config);

		AuthResult Authenticate(AuthRequest authRequest);
	}
}