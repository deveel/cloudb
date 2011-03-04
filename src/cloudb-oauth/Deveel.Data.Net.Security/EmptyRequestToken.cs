using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public class EmptyRequestToken : EmptyToken, IRequestToken {
		private readonly OAuthParameters associatedParameters = new OAuthParameters();
		private TokenStatus status;
		private string[] roles;

		public EmptyRequestToken(string consumerKey)
			: base(consumerKey, TokenType.Request) {
			status = TokenStatus.Authorized;
		}

		public OAuthParameters AssociatedParameters {
			get { return associatedParameters; }
		}

		public IIdentity AuthenticatedUser {
			get { return null; }
		}

		public string[] Roles {
			get { return roles; }
			set { roles = value; }
		}

		public TokenStatus Status {
			get { return status; }
		}

		public void ChangeStatus(TokenStatus newStatus) {
			status = newStatus;
		}
	}
}