using System;

namespace Deveel.Data.Net.Security {
	 class OAuthRequestTokenIssuer : OAuthTokenIssuer {
		public OAuthRequestTokenIssuer(OAuthProvider provider) 
			: base(provider) {
		}

		protected override void SetRequestToken(OAuthRequestContext requestContext) {
		}

		protected override void CheckVerifier(OAuthRequestContext requestContext) {
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
			parameters.RequireAllOf(OAuthParameterKeys.ConsumerKey, OAuthParameterKeys.SignatureMethod,
			                        OAuthParameterKeys.Signature, OAuthParameterKeys.Timestamp, OAuthParameterKeys.Nonce,
			                        OAuthParameterKeys.Callback);

			// The version parameter is optional, but it if is present its value must be 1.0
			if (parameters.Version != null)
				parameters.RequireVersion("1.0");

			requestContext.Parameters = parameters;
		}

		protected override void IssueToken(IHttpContext httpContext, OAuthRequestContext requestContext) {
			// Generate a request token
			IRequestToken token = GenerateToken(TokenType.Request, httpContext, requestContext) as IRequestToken;

			// Check to see if the request for a token is oob and that oob is allowed.
			if (requestContext.Parameters.Callback.Equals("oob")) {
				if (!Provider.AllowOutOfBandCallback)
					throw new ParametersRejectedException("Out of band is not supported.", new string[] { OAuthParameterKeys.Callback });
			} else {
				Uri callbackUri;

				if (!Uri.TryCreate(requestContext.Parameters.Callback, UriKind.Absolute, out callbackUri))
					throw new ParametersRejectedException("Not a valid Uri.", new string[] { OAuthParameterKeys.Callback });

				Provider.CallbackStore.SaveCallback(token, callbackUri);
			}

			// Store the token
			requestContext.RequestToken = token;
			Provider.TokenStore.Add(token);

			// Add to the response
			requestContext.ResponseParameters[OAuthParameterKeys.CallbackConfirmed] = "true"; // The spec never defines when to send false or what will happen if you do.
			requestContext.ResponseParameters[OAuthParameterKeys.Token] = token.Token;
			requestContext.ResponseParameters[OAuthParameterKeys.TokenSecret] = token.Secret;
		}
	}
}