using System;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthConsumer : IConsumer {
		private readonly string key;
		private readonly string secret;
		private ConsumerStatus status;
		private bool statusChanged;

		public OAuthConsumer(string key, string secret, ConsumerStatus status) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");
			if (String.IsNullOrEmpty(secret))
				throw new ArgumentNullException("secret");

			this.key = key;
			this.status = status;
			this.secret = secret;
		}

		public OAuthConsumer(string key, string secret)
			: this(key, secret, ConsumerStatus.Unknown) {
		}

		public string Key {
			get { return key; }
		}

		public string Secret {
			get { return secret; }
		}

		public ConsumerStatus Status {
			get { return status; }
		}

		public bool StatusChanged {
			get { return statusChanged; }
		}

		public void ChangeStatus(ConsumerStatus newStatus) {
			statusChanged = (status != newStatus);
			status = newStatus;
		}

		public override bool Equals(object obj) {
			if (ReferenceEquals(this, obj))
				return true;

			OAuthConsumer other = obj as OAuthConsumer;

			if (other == null)
				return false;

			return Equals(other);
		}

		public bool Equals(OAuthConsumer other) {
			return other != null &&
			       String.Equals(key, other.Key) &&
			       String.Equals(secret, other.Secret) &&
			       status == other.Status;
		}

		public override int GetHashCode() {
			return key.GetHashCode() ^ secret.GetHashCode() ^ status.GetHashCode();
		}
	}
}