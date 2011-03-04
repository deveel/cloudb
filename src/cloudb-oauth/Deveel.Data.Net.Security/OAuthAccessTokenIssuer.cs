using System;

namespace Deveel.Data.Net.Security {
	public class OAuthAccessTokenIssuer : OAuthTokenIssuer {
		protected override void ParseParameters(IHttpContext httpContext, OAuthRequestContext requestContext) {
			// Try to parse the parameters
			OAuthParameters parameters = OAuthParameters.Parse(httpContext.Request, OAuthParameterSources.ServiceProviderDefault);

			/*
			 * Check for missing required parameters:
			 * 
			 * The consumer key, signature method, signature, timestamp and nonce parameters
			 * are all required
			 */
			parameters.RequireAllOf(
					OAuthParameterKeys.ConsumerKey,
					OAuthParameterKeys.Token,
					OAuthParameterKeys.SignatureMethod,
					OAuthParameterKeys.Signature,
					OAuthParameterKeys.Timestamp,
					OAuthParameterKeys.Nonce,
					OAuthParameterKeys.Verifier);

			/*
			 * The version parameter is optional, but it if is present its value must be 1.0
			 */
			if (parameters.Version != null)
				parameters.RequireVersion("1.0");

			/*
			 * Check that there are no other parameters except for realm, version and
			 * the required parameters
			 */
			parameters.AllowOnly(
					OAuthParameterKeys.ConsumerKey,
					OAuthParameterKeys.Token,
					OAuthParameterKeys.SignatureMethod,
					OAuthParameterKeys.Signature,
					OAuthParameterKeys.Timestamp,
					OAuthParameterKeys.Nonce,
					OAuthParameterKeys.Verifier,
					OAuthParameterKeys.Version, // (optional)
					OAuthParameterKeys.Realm); // (optional)

			requestContext.Parameters = parameters;

		}

		protected override void IssueToken(IHttpContext httpContext, OAuthRequestContext requestContext) {
			throw new NotImplementedException();
		}
	}
}