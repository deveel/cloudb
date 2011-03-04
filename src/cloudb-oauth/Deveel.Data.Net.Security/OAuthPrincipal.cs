using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthPrincipal : IPrincipal {
		private readonly IAccessToken accessToken;
		private readonly IRequestToken requestToken;
		private readonly IIdentity identity;

		public OAuthPrincipal(IAccessToken accessToken) {
			if (accessToken == null)
				throw new ArgumentNullException("accessToken");

			if (accessToken.RequestToken == null)
				throw new ArgumentException("Access token must have a request token", "accessToken");

			if (accessToken.RequestToken.AuthenticatedUser == null)
				throw new ArgumentException("Request token must have an authenticated user", "accessToken");

			this.accessToken = accessToken;
			requestToken = accessToken.RequestToken;
			identity = RequestToken.AuthenticatedUser;
		}

		public IAccessToken AccessToken {
			get { return accessToken; }
		}

		public bool IsInRole(string role) {
			return requestToken.Roles != null && Array.IndexOf(requestToken.Roles, role) >= 0;
		}

		public IIdentity Identity {
			get { return identity; }
		}

		public IRequestToken RequestToken {
			get { return requestToken; }
		}
	}
}