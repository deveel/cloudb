using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public class OAuthService {
		private readonly OAuthEndPoint requestTokenEndPoint;
		private readonly OAuthEndPoint accessTokenEndPoint;
		private readonly Uri authorizationUrl;
		private readonly bool useAuthorizationHeader;
		private readonly string realm;
		private readonly string signatureMethod;
		private readonly string version;
		private readonly IConsumer consumer;

		public OAuthEndPoint RequestTokenEndPoint {
			get { return requestTokenEndPoint; }
		}

		public Uri RequestTokenUrl {
			get { return requestTokenEndPoint.Uri; }
		}

		public Uri AuthorizationUrl {
			get { return authorizationUrl; }
		}

		public OAuthEndPoint AccessTokenEndPoint {
			get { return accessTokenEndPoint; }
		}

		public Uri AccessTokenUrl {
			get { return accessTokenEndPoint.Uri; }
		}

		public bool UseAuthorizationHeader {
			get { return useAuthorizationHeader; }
		}

		public string Realm {
			get { return realm; }
		}

		public string SignatureMethod {
			get { return signatureMethod; }
		}

		public string OAuthVersion {
			get { return version; }
		}

		public IConsumer Consumer {
			get { return consumer; }
		}

		public OAuthService(OAuthEndPoint requestTokenEndPoint, Uri authorizationUrl, OAuthEndPoint accessTokenEndPoint, IConsumer consumer)
			: this(requestTokenEndPoint, authorizationUrl, accessTokenEndPoint, true, null, "HMAC-SHA1", "1.0", consumer) {
		}

		public OAuthService(OAuthEndPoint requestTokenEndPoint, Uri authorizationUrl, OAuthEndPoint accessTokenEndPoint, string signatureMethod, IConsumer consumer)
			: this(requestTokenEndPoint, authorizationUrl, accessTokenEndPoint, true, null, signatureMethod, "1.0", consumer) {
		}

		public OAuthService (OAuthEndPoint requestTokenEndPoint, Uri authorizationUrl, OAuthEndPoint accessTokenEndPoint, bool useAuthorizationHeader, string realm, string signatureMethod, string oauthVersion, IConsumer consumer) {
			this.requestTokenEndPoint = requestTokenEndPoint;
			this.authorizationUrl = authorizationUrl;
			this.accessTokenEndPoint = accessTokenEndPoint;
			this.useAuthorizationHeader = useAuthorizationHeader;
			this.realm = realm;
			this.signatureMethod = signatureMethod;
			version = oauthVersion;
			this.consumer = consumer;
		}

		public static bool operator ==(OAuthService left, OAuthService right) {
			if (ReferenceEquals(left, right))
				return true;

			if (((object)left) == null && ((object)right) == null)
				return true;

			return left.Equals(right);
		}

		public static bool operator !=(OAuthService left, OAuthService right) {
			return !(left == right);
		}

		public Uri BuildAuthorizationUrl(IToken token) {
			return BuildAuthorizationUrl(token, null);
		}


		public Uri BuildAuthorizationUrl(IToken token, NameValueCollection additionalParameters) {
			if (token.Type == TokenType.Request) {
				OAuthParameters authParameters = new OAuthParameters();
				authParameters.Token = token.Token;

				if (additionalParameters != null)
					authParameters.AdditionalParameters.Add(additionalParameters);

				// Construct final authorization Uri (HTTP method must be GET)
				string query = authParameters.ToQueryString();

				UriBuilder authUri = new UriBuilder(AuthorizationUrl);

				if (String.IsNullOrEmpty(authUri.Query))
					authUri.Query = query;
				else
					authUri.Query = authUri.Query.Substring(1) + "&" + query;

				return authUri.Uri;
			}

			throw new ArgumentException("Invalid token type for Authorization");
		}

		public override int GetHashCode() {
			return requestTokenEndPoint.GetHashCode() ^ authorizationUrl.GetHashCode() ^ accessTokenEndPoint.GetHashCode() ^
			       realm.GetHashCode() ^ signatureMethod.GetHashCode() ^ version.GetHashCode() ^ consumer.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (ReferenceEquals(this, obj))
				return true;

			OAuthService other = obj as OAuthService;

			if (other == null)
				return false;

			return Equals(other);
		}

		public bool Equals(OAuthService other) {
			return other != null && requestTokenEndPoint.Equals(other.RequestTokenUrl) &&
			       authorizationUrl.Equals(other.AuthorizationUrl) && accessTokenEndPoint.Equals(other.AccessTokenUrl) &&
			       useAuthorizationHeader == other.UseAuthorizationHeader && String.Equals(realm, other.Realm) &&
			       String.Equals(signatureMethod, other.SignatureMethod) && String.Equals(version, other.OAuthVersion) &&
			       consumer.Equals(other.Consumer);
		}
	}
}