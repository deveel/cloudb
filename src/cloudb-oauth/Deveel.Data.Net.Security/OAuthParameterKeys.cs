using System;

namespace Deveel.Data.Net.Security {
	public static class OAuthParameterKeys {
		public const string ConsumerKey = "oauth_consumer_key";
		public const string SignatureMethod = "oauth_signature_method";
		public const string Signature = "oauth_signature";
		public const string Timestamp = "oauth_timestamp";
		public const string Nonce = "oauth_nonce";
		public const string Version = "oauth_version";
		public const string Verifier = "oauth_verifier";
		public const string Token = "oauth_token";
		public const string TokenSecret = "oauth_token_secret";
		public const string Callback = "oauth_callback";
		public const string CallbackConfirmed = "oauth_callback_confirmed";
		public const string Realm = "realm";
	}
}