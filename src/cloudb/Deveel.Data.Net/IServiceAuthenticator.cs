using System;
using System.IO;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public interface IServiceAuthenticator {
		bool Authenticate(AuthenticationPoint point, Stream stream);
	}
}