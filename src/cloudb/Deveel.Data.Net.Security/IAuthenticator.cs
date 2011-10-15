using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface IAuthenticator {
		void Init(ConfigSource config);

		void CollectData(IDictionary<string,AuthObject> authData);

		AuthResult Authenticate(AuthRequest authRequest);

		void EndContext(object context);
	}
}