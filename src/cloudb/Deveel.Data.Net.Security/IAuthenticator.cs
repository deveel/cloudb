using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface IAuthenticator : IConfigurable {
		AuthResult Authenticate(AuthRequest authRequest);
	}
}