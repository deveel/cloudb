using System;

namespace Deveel.Data.Net.Security {
	public static class OAuthProblemTypes {
		public const string VersionRejected = "version_rejected";
		public const string ParameterAbsent = "parameter_absent";
		public const string ParameterRejected = "parameter_rejected";
		public const string TimestampRefused = "timestamp_refused";
		public const string NonceUsed = "nonce_used";
		public const string SignatureMethodRejected = "signature_method_rejected";
		public const string SignatureInvalid = "signature_invalid";
		public const string ConsumerKeyUnknown = "consumer_key_unknown";
		public const string ConsumerKeyRejected = "consumer_key_rejected";
		public const string ConsumerKeyRefused = "consumer_key_refused";
		public const string TokenUsed = "token_used";
		public const string TokenExpired = "token_expired";
		public const string TokenRevoked = "token_revoked";
		public const string TokenRejected = "token_rejected";
		public const string PermissionUnknown = "permission_unknown";
		public const string PermissionDenied = "permission_denied";
	}
}