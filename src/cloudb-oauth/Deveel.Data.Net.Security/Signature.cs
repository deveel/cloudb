using System;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	public static class Signature {
		public static string Create(string httpMethod, Uri requestUrl, OAuthParameters parameters) {
			StringBuilder sigbase = new StringBuilder();

			// Http header
			sigbase.Append(Rfc3986.Encode(httpMethod)).Append("&");

			// Normalized request URL
			sigbase.Append(Rfc3986.Encode(requestUrl.Scheme));
			sigbase.Append(Rfc3986.Encode("://"));
			sigbase.Append(Rfc3986.Encode(requestUrl.Authority.ToLowerInvariant()));
			sigbase.Append(Rfc3986.Encode(requestUrl.AbsolutePath));
			sigbase.Append("&");

			// Normalized parameters
			sigbase.Append(
				Rfc3986.Encode(parameters.ToNormalizedString(OAuthParameterKeys.Realm, OAuthParameterKeys.Signature,
				                                             OAuthParameterKeys.TokenSecret)));

			return sigbase.ToString();
		}
	}
}