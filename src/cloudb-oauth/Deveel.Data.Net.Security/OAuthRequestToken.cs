using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthRequestToken : IRequestToken {
		private readonly string token;
		private readonly string secret;
		private TokenStatus status;
		private readonly string consumerKey;
		private readonly OAuthParameters parameters;
		private readonly IIdentity user;
		private readonly string[] roles;
		private bool statusChanged;

		public OAuthRequestToken(string token, string secret, IConsumer consumer, TokenStatus status, OAuthParameters parameters, IIdentity user, string[] roles) {
			if (string.IsNullOrEmpty(token))
				throw new ArgumentException("token must not be null or empty", "token");

			if (secret == null)
				throw new ArgumentNullException("secret", "secret must not be null");

			if (consumer == null)
				throw new ArgumentNullException("consumer", "consumer must not be null");

			if (roles == null)
				throw new ArgumentNullException("roles", "roles must not be null");

			this.token = token;
			this.secret = secret;
			this.status = status;
			consumerKey = consumer.Key;
			this.parameters = parameters;
			this.user = user;
			this.roles = roles;
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
			get { return TokenType.Request; }
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

		public OAuthParameters AssociatedParameters {
			get { return parameters; }
		}

		public IIdentity AuthenticatedUser {
			get { return user; }
		}

		public string[] Roles {
			get { return roles; }
		}

		private bool RolesEquals(string[] otherRoles) {
			if (roles == null && otherRoles == null)
				return true;

			if (roles == null)
				return false;

			if (roles.Length != otherRoles.Length)
				return false;

			for (int i = 0; i < roles.Length; i++) {
				if (!String.Equals(roles[i], otherRoles[i]))
					return false;
			}

			return true;
		}

		public override int GetHashCode() {
			return token.GetHashCode() ^ secret.GetHashCode() ^ consumerKey.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (obj == null)
				return false;

			if (ReferenceEquals(this, obj))
				return true;

			if (!(obj is IRequestToken))
				return false;

			return Equals(obj as IRequestToken);
		}

		public bool Equals(IRequestToken other) {
			if (other == null)
				return false;

			return token.Equals(other.Token) &&
			       secret.Equals(other.Secret) &&
			       status == other.Status &&
			       String.Equals(consumerKey, other.ConsumerKey) &&
			       ((parameters == null && other.AssociatedParameters == null) ||
			        (parameters != null && parameters.Equals(other.AssociatedParameters))) &&
			       ((user == null && other.AuthenticatedUser == null) ||
			        (user != null && user.Equals(other.AuthenticatedUser))) &&
			       RolesEquals(other.Roles);
		}
	}
}