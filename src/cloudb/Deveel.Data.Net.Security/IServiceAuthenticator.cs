using System;

namespace Deveel.Data.Net.Security {
	public interface IServiceAuthenticator : IAuthenticator {
		AuthRequest CreateRequest(AuthResponse authResponse);
	}
}