using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class NoAuthenticationAuthenticator : IServiceAuthenticator {
		public static readonly NoAuthenticationAuthenticator Instance = new NoAuthenticationAuthenticator();

		private NoAuthenticationAuthenticator() {
		}

		public bool Authenticate(AuthenticationPoint point, Stream stream) {
			return true;
		}
	}
}