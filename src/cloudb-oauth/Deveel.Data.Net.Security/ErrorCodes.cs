using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	internal static class ErrorCodes {
		private static readonly Dictionary<string, int> codes = new Dictionary<string, int>();

		static ErrorCodes() {
			codes.Add(OAuthProblemTypes.VersionRejected, VersionRejected);
			codes.Add(OAuthProblemTypes.ParameterAbsent, ParameterAbsent);
			codes.Add(OAuthProblemTypes.ParameterRejected, ParameterRejected);
			codes.Add(OAuthProblemTypes.TimestampRefused, TimestampRefused);
			codes.Add(OAuthProblemTypes.NonceUsed, NonceUsed);
			codes.Add(OAuthProblemTypes.SignatureMethodRejected, SignatureMethodRejected);
			codes.Add(OAuthProblemTypes.SignatureInvalid, SignatureInvalid);
			codes.Add(OAuthProblemTypes.ConsumerKeyUnknown, ConsumerKeyUnknown);
			codes.Add(OAuthProblemTypes.ConsumerKeyRejected, ConsumerKeyRejected);
			codes.Add(OAuthProblemTypes.ConsumerKeyRefused, ConsumerKeyRefused);
			codes.Add(OAuthProblemTypes.TokenUsed, TokenUsed);
			codes.Add(OAuthProblemTypes.TokenExpired, TokenExpired);
			codes.Add(OAuthProblemTypes.TokenRevoked, TokenRevoked);
			codes.Add(OAuthProblemTypes.TokenRejected, TokenRejected);
			codes.Add(OAuthProblemTypes.PermissionUnknown, PermissionUnknown);
			codes.Add(OAuthProblemTypes.PermissionDenied, PermissionDenied);
		}

		public const int VersionRejected = 0x01001;
		public const int ParameterAbsent = 0x01010;
		public const int ParameterRejected = 0x01011;
		public const int TimestampRefused = 0x01020;
		public const int NonceUsed = 0x01030;
		public const int SignatureMethodRejected = 0x01030;
		public const int SignatureInvalid = 0x01031;
		public const int ConsumerKeyUnknown = 0x01040;
		public const int ConsumerKeyRejected = 0x01041;
		public const int ConsumerKeyRefused = 0x01042;
		public const int TokenUsed = 0x01050;
		public const int TokenExpired = 0x01051;
		public const int TokenRevoked = 0x01052;
		public const int TokenRejected = 0x01053;
		public const int PermissionUnknown = 0x01060;
		public const int PermissionDenied = 0x01061;

		// Unknown
		public const int Unknown = 0x10000;

		public static int GetCode(string problemType) {
			int code;
			if (!codes.TryGetValue(problemType, out code))
				code = Unknown;
			return code;
		}
	}
}