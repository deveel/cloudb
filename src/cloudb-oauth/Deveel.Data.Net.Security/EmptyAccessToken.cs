using System;

namespace Deveel.Data.Net.Security {
	public class EmptyAccessToken : EmptyToken, IAccessToken {
		private readonly IRequestToken requestToken;
		private TokenStatus status;

		public EmptyAccessToken(string consumerKey)
			: base(consumerKey, TokenType.Access) {
			requestToken = new EmptyRequestToken(consumerKey);
			status = TokenStatus.Authorized;
		}

		public IRequestToken RequestToken {
			get { return requestToken; }
		}

		public TokenStatus Status {
			get { return status; }
		}

		public void ChangeStatus(TokenStatus newStatus) {
			status = newStatus;
		}
	}
}