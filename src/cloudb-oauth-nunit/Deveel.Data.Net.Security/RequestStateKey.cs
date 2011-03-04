using System;

namespace Deveel.Data.Net.Security {
	public class RequestStateKey : IEquatable<RequestStateKey> {
		private readonly string serviceRealm;
		private readonly string consumerKey;
		private readonly string endUserId;

		public RequestStateKey(string serviceRealm, string consumerKey, string endUserId) {
			if (String.IsNullOrEmpty(serviceRealm))
				throw new ArgumentNullException("serviceRealm");

			if (String.IsNullOrEmpty(consumerKey))
				throw new ArgumentNullException("consumerKey");

			this.serviceRealm = serviceRealm;
			this.consumerKey = consumerKey;
			this.endUserId = endUserId;
		}

		public RequestStateKey(OAuthService service, string endUserId) {
			if (service == null)
				throw new ArgumentNullException("service");

			string serviceRealm = null;
			if (!String.IsNullOrEmpty(service.Realm))
				serviceRealm = service.Realm;
			else if (service.AuthorizationUrl != null)
				serviceRealm = service.AuthorizationUrl.AbsoluteUri;
			else
				throw new ArgumentException("Service does not have realm or authorization URI", "service");

			if (service.Consumer == null || String.IsNullOrEmpty(service.Consumer.Key))
				throw new ArgumentException("Service does not have consumer key", "service");

			this.serviceRealm = serviceRealm;
			consumerKey = service.Consumer.Key;
			this.endUserId = endUserId;
		}

		public string ServiceRealm {
			get { return serviceRealm; }
		}

		public string ConsumerKey {
			get { return consumerKey; }
		}

		public string EndUserId {
			get { return endUserId; }
		}

		public override bool Equals(object obj) {
			var other = obj as RequestStateKey;

			if (obj == null)
				return false;

			return Equals(other);
		}

		public bool Equals(RequestStateKey other) {
			if (other == null)
				return false;

			return String.Equals(serviceRealm, other.ServiceRealm, StringComparison.Ordinal) &&
			       String.Equals(consumerKey, other.ConsumerKey, StringComparison.Ordinal) &&
			       String.Equals(endUserId, other.EndUserId, StringComparison.Ordinal);
		}

		public override int GetHashCode() {
			int hashCode = serviceRealm.GetHashCode() ^ consumerKey.GetHashCode();

			if (!String.IsNullOrEmpty(endUserId))
				hashCode ^= endUserId.GetHashCode();

			return hashCode;
		}
	}
}