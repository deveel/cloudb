using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface IAuthenticator {
		string Mechanism { get; }


		void Init(ConfigSource config);

		AuthResponse Authenticate(AuthRequest authRequest);

		void EndContext(object context);
	}
}