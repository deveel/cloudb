using System;

namespace Deveel.Data.Net.Security {
	public struct OAuthEndPoint {
		private readonly Uri uri;
		private readonly string httpMethod;

		public OAuthEndPoint(string uri)
			: this(new Uri(uri)) {
		}

		public OAuthEndPoint(Uri uri)
			: this(uri, "GET") {
		}

		public OAuthEndPoint(string uri, string httpMethod)
			: this(new Uri(uri), httpMethod) {
		}

		public OAuthEndPoint(Uri uri, string httpMethod) {
			if (!uri.IsAbsoluteUri)
				throw new ArgumentException("The uri of this end-point must be absolute.");
			if (String.IsNullOrEmpty(httpMethod))
				throw new ArgumentNullException("httpMethod");

			if (String.Compare(httpMethod, "GET", true) != 0 &&
			    String.Compare(httpMethod, "POST", true) != 0 &&
			    String.Compare(httpMethod, "DELETE", true) != 0 &&
			    String.Compare(httpMethod, "PUT", true) != 0)
				throw new ArgumentException("Only GET, POST, DELETE AND PUT are supported HttpMethods", "httpMethod");

			this.httpMethod = httpMethod;
			this.uri = uri;
		}

		public Uri Uri {
			get { return uri; }
		}

		public string HttpMethod {
			get { return httpMethod; }
		}

		public static bool operator ==(OAuthEndPoint left, OAuthEndPoint right) {
			return left.Equals(right);
		}

		public static bool operator !=(OAuthEndPoint left, OAuthEndPoint right) {
			return !(left == right);
		}

		public override int GetHashCode() {
			return httpMethod.GetHashCode() ^ uri.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (obj == null)
				return false;

			if (!(obj is OAuthEndPoint))
				return false;

			return Equals((OAuthEndPoint)obj);
		}

		public bool Equals(OAuthEndPoint other) {
			return string.Equals(httpMethod, other.HttpMethod) &&
			       ((uri == null && other.Uri == null) || (uri != null && uri.Equals(other.Uri)));
		}
	}
}