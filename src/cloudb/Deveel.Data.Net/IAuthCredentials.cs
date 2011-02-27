using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public interface IAuthCredentials {
		string AuthMethod { get; }

		IDictionary<string, object> AuthData { get; }
	}
}