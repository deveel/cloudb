using System;

namespace Deveel.Data.Net.Security {
	public sealed class RequestId {
		private readonly long timestamp;
		private readonly string nonce;
		private readonly string token;
		private readonly string consumerKey;

		public RequestId(long timestamp, string nonce, string consumerKey, string token) {
			this.timestamp = timestamp;
			this.nonce = nonce;
			this.consumerKey = consumerKey;
			this.token = token;
		}

		public long Timestamp {
			get { return timestamp; }
		}

		public string Nonce {
			get { return nonce; }
		}

		public string ConsumerKey {
			get { return consumerKey; }
		}

		public string Token {
			get { return token; }
		}

		public override int GetHashCode() {
			return timestamp.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (obj == null)
				return false;

			if (ReferenceEquals(this, obj))
				return true;

			if (!(obj is RequestId))
				return false;

			return Equals((RequestId)obj);
		}

		private bool Equals(RequestId other) {
			return timestamp == other.Timestamp &&
			       String.Equals(nonce, other.Nonce) &&
			       String.Equals(consumerKey, other.ConsumerKey) &&
			       String.Equals(token, other.Token);
		}
	}

}