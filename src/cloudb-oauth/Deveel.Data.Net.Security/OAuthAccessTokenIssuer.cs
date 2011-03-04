using System;

namespace Deveel.Data.Net.Security {
	class OAuthAccessTokenIssuer : OAuthTokenIssuer {
		public OAuthAccessTokenIssuer(OAuthProvider provider) 
			: base(provider) {
		}

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
			// Generate an access token
			IAccessToken accessToken = GenerateToken(TokenType.Access, httpContext, requestContext) as IAccessToken;
			if (accessToken == null)
				throw new InvalidOperationException();

			// Mark the token as authorized
			accessToken.ChangeStatus(TokenStatus.Authorized);

			// Don't store the token
			// Don't mark the request token as used

			// Add to the response
			requestContext.ResponseParameters[OAuthParameterKeys.Token] = accessToken.Token;
			requestContext.ResponseParameters[OAuthParameterKeys.TokenSecret] = accessToken.Secret;
		}
	}
}