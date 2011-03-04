using System;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthAccessToken : IAccessToken {
		private readonly string token;
		private readonly string secret;
		private TokenStatus status;
		private bool statusChanged;
		private readonly string consumerKey;
		private readonly IRequestToken requestToken;

		public OAuthAccessToken(string token, string secret, IConsumer consumer, TokenStatus status, IRequestToken requestToken) {
			if (string.IsNullOrEmpty(token))
				throw new ArgumentException("token must not be null or empty", "token");

			if (secret == null)
				throw new ArgumentNullException("secret", "secret must not be null");

			if (consumer == null)
				throw new ArgumentNullException("consumer", "consumer must not be null");

			if (requestToken == null)
				throw new ArgumentNullException("requestToken", "requestToken must not be null");

			this.token = token;
			this.secret = secret;
			this.status = status;
			consumerKey = consumer.Key;
			this.requestToken = requestToken;
		}

		public string Token {
			get { return token; }
		}

		public string Secret {
			get { return secret; }
		}

		public string ConsumerKey {
			get { return consumerKey; }
		}

		TokenType IToken.Type {
			get { return TokenType.Access; }
		}

		public TokenStatus Status {
			get { return status; }
		}

		public bool StatusChanged {
			get { return statusChanged; }
		}

		public void ChangeStatus(TokenStatus newStatus) {
			statusChanged = (status != newStatus);
			status = newStatus;
		}

		public IRequestToken RequestToken {
			get { return requestToken; }
		}

		public override int GetHashCode() {
			// If the token is set then all parameters must have a value as they would have been required in the constructor.            
			if (token != null)
				return token.GetHashCode() ^ secret.GetHashCode() ^ consumerKey.GetHashCode();
			return base.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (ReferenceEquals(this, obj))
				return true;

			IAccessToken other = obj as IAccessToken;
			if (other == null)
				return false;

			return Equals(other);
		}

		private bool Equals(IAccessToken other) {
			if (other == null)
				return false;

			return String.Equals(token, other.Token) &&
			       String.Equals(secret, other.Secret) &&
			       status == other.Status &&
			       String.Equals(consumerKey, other.ConsumerKey) &&
			       ((requestToken == null && other.RequestToken == null) ||
			        (requestToken != null && requestToken.Equals(other.RequestToken)));
		}
	}
}