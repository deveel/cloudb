using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	abstract class OAuthTokenIssuer {
		private readonly OAuthProvider provider;

		internal OAuthTokenIssuer(OAuthProvider provider) {
			this.provider = provider;
		}

		public OAuthProvider Provider {
			get { return provider; }
		}

		private static void SetSignProvider(OAuthRequestContext context) {
			ISignProvider signingProvider = SignProviders.GetProvider(context.Parameters.SignatureMethod);

			if (signingProvider == null)
				// There is no signing provider for this signature method
				throw new OAuthRequestException(null, OAuthProblemTypes.SignatureMethodRejected);

			// Double check the signing provider declares that it can handle the signature method
			if (!signingProvider.SignatureMethod.Equals(context.Parameters.SignatureMethod))
				throw new OAuthRequestException(null, OAuthProblemTypes.SignatureMethodRejected);

			context.SignProvider = signingProvider;
		}

		private static void SetSignature(IHttpContext httpContext, OAuthRequestContext requestContext) {
			// Get the token to sign with
			string tokenSecret;

			if (requestContext.AccessToken != null)
				tokenSecret = requestContext.AccessToken.Secret;
			else if (requestContext.RequestToken != null)
				tokenSecret = requestContext.RequestToken.Secret;
			else
				tokenSecret = null;

			bool isValid = requestContext.SignProvider.ValidateSignature(
				Signature.Create(httpContext.Request.HttpMethod,
								 new Uri(httpContext.Request.Url.GetLeftPart(UriPartial.Authority) + httpContext.Request.RawUrl),
								 requestContext.Parameters), requestContext.Parameters.Signature, requestContext.Consumer.Secret, tokenSecret);

			if (!isValid)
				throw new OAuthRequestException(null, OAuthProblemTypes.SignatureInvalid);

			requestContext.IsSignatureValid = true;
		}
		
		protected virtual void SetConsumer(OAuthRequestContext context) {
			IConsumer consumer = provider.ConsumerStore.Get(context.Parameters.ConsumerKey);
			if (consumer == null)
				throw new OAuthRequestException(null, OAuthProblemTypes.ConsumerKeyUnknown);

			switch (consumer.Status) {
				case ConsumerStatus.Valid:
					context.Consumer = consumer;
					break;

				case ConsumerStatus.TemporarilyDisabled:
					throw new OAuthRequestException(null, OAuthProblemTypes.ConsumerKeyRefused);
				case ConsumerStatus.PermanentlyDisabled:
					throw new OAuthRequestException(null, OAuthProblemTypes.ConsumerKeyRejected);
				case ConsumerStatus.Unknown:
				default:
					throw new OAuthRequestException(null, OAuthProblemTypes.ConsumerKeyUnknown);
			}
		}

		protected IToken GenerateToken(TokenType tokenType, IHttpContext httpContext, OAuthRequestContext requestContext) {
			IToken token;
			do {
				if (tokenType == TokenType.Request) {
					token = provider.TokenGenerator.CreateRequestToken(requestContext.Consumer, requestContext.Parameters);
				} else {
					token = provider.TokenGenerator.CreateAccessToken(requestContext.Consumer, requestContext.RequestToken);
				}
			}
			while (provider.TokenStore.Get(token.Token, tokenType) != null);

			return token;
		}

		protected virtual bool AllowRequest(IHttpContext context, OAuthRequestContext authContext) {
			bool allow = true;
			if (provider.ResourceAccessVerifier != null)
				allow = provider.ResourceAccessVerifier.VerifyAccess(context, authContext);
			return allow;
		}

		protected virtual void SetRequestId(OAuthRequestContext context) {
			long timestamp = Int64.Parse(context.Parameters.Timestamp);
			context.RequestId = provider.RequestIdValidator.ValidateRequest(context.Parameters.Nonce, timestamp,
																   context.Parameters.ConsumerKey, context.Parameters.Token);
		}

		protected virtual void SetRequestToken(OAuthRequestContext requestContext) {
			IRequestToken token = provider.TokenStore.Get(requestContext.Parameters.Token, TokenType.Request) as IRequestToken;
			if (token == null)
				throw new OAuthRequestException(null, OAuthProblemTypes.TokenRejected);

			if (!token.ConsumerKey.Equals(requestContext.Parameters.ConsumerKey))
				throw new OAuthRequestException(null, OAuthProblemTypes.TokenRejected);

			switch (token.Status) {
				case TokenStatus.Authorized:
					requestContext.RequestToken = token;
					break;

				case TokenStatus.Expired:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenExpired);
				case TokenStatus.Used:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenUsed);
				case TokenStatus.Revoked:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenRevoked);
				case TokenStatus.Unauthorized:
				case TokenStatus.Unknown:
				default:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenRejected);
			}
		}

		private static void AddApplicationResponseParameters(OAuthRequestContext requestContext, NameValueCollection additionalParameters) {
			if (additionalParameters == null)
				return;

			// Remove any oauth_ prefixed parameters from the application's additional response
			foreach (string key in additionalParameters.AllKeys)
				if (key.StartsWith(OAuthParameterKeys.OAuthParameterPrefix, StringComparison.Ordinal))
					additionalParameters.Remove(key);

			// Add the application's custom parameters
			requestContext.ResponseParameters.Add(additionalParameters);
		}


		protected abstract void ParseParameters(IHttpContext request, OAuthRequestContext requestContext);

		protected virtual NameValueCollection GetAdditionalResponseParameters(IHttpContext httpContext, OAuthRequestContext requestContext) {
			// By default, there are no extra parameters
			return null;
		}

		protected virtual void CheckVerifier(OAuthRequestContext requestContext) {
			if (!provider.VerificationProvider.IsValid(requestContext.RequestToken, requestContext.Parameters.Verifier))
				throw new ParametersRejectedException("Invalid verifier for request token.", new string[] { OAuthParameterKeys.Verifier });
		}

		protected abstract void IssueToken(IHttpContext httpContext, OAuthRequestContext requestContext);

		public TokenIssueResult ProcessIssueRequest(IHttpContext context) {
			OAuthRequestContext authContext = new OAuthRequestContext();

			// Check request parameters
			try {
				// TODO: Should we ensure the realm parameter, if present, matches the configured realm?
				ParseParameters(context, authContext);
				SetSignProvider(authContext);
				SetConsumer(authContext);
				SetRequestId(authContext);
				SetRequestToken(authContext);
				SetSignature(context, authContext);
				CheckVerifier(authContext);
			} catch (OAuthRequestException ex) {
				authContext.AddError(ex);

				TokenIssueResult error = new TokenIssueResult(false, ex.Message, ex.Problem);
				error.Parameters.Add(authContext.ResponseParameters);
				return error;
			}

			// Allow the application to decide whether to issue the access token
			bool isRequestAllowed = AllowRequest(context, authContext);

			if (isRequestAllowed) {
				// Allow the application to add additional response parameters
				AddApplicationResponseParameters(authContext, GetAdditionalResponseParameters(context, authContext));

				// Issue the token
				IssueToken(context, authContext);

				TokenIssueResult result = new TokenIssueResult(true);
				result.Parameters.Add(authContext.ResponseParameters);
				return result;
			} else {
				TokenIssueResult error = new TokenIssueResult(false);
				error.Parameters.Add(authContext.ResponseParameters);
				return error;
			}
		}
	}
}