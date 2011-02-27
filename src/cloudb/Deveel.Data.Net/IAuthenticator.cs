using System;

namespace Deveel.Data.Net {
	public interface IAuthenticator {
		bool IsAuthenticated { get; }

		AuthResult Authenticate();
	}
}