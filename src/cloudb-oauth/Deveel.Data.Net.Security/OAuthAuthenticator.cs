using System;
using System.Collections.Generic;
using System.Collections.Specialized;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public class OAuthAuthenticator : IAuthenticator {
		private bool consumerRequests;
		private IPathAccessVerifier accessVerifier;

		public IPathAccessVerifier PathAccessVerifier {
			get { return accessVerifier; }
			set { accessVerifier = value; }
		}

		private void ParseParameters(IHttpContext httpContext, OAuthRequestContext context) {
			// Try to parse the parameters
			OAuthParameters parameters = OAuthParameters.Parse(httpContext.Request);

			/*
			 * Check for missing required parameters:
			 * 
			 * The consumer key, token, signature method, signature, timestamp and nonce parameters
			 * are all required
			 */
			if (consumerRequests) {
				parameters.RequireAllOf(
						OAuthParameterKeys.ConsumerKey,
						OAuthParameterKeys.SignatureMethod,
						OAuthParameterKeys.Signature,
						OAuthParameterKeys.Timestamp,
						OAuthParameterKeys.Nonce);
			} else {
				// For 3 legged TokenParameter is required
				parameters.RequireAllOf(
						OAuthParameterKeys.ConsumerKey,
						OAuthParameterKeys.Token,
						OAuthParameterKeys.SignatureMethod,
						OAuthParameterKeys.Signature,
						OAuthParameterKeys.Timestamp,
						OAuthParameterKeys.Nonce);
			}

			/*
			 * The version parameter is optional, but it if is present its value must be 1.0
			 */
			if (parameters.Version != null)
				parameters.RequireVersion("1.0");

			context.Parameters = parameters;
		}

		private void SetAccessToken(OAuthRequestContext context) {
			IAccessToken accessToken;

			if (context.Parameters.Token == null && consumerRequests) {
				accessToken = new EmptyAccessToken(context.Consumer.Key);
			} else if ((accessToken = (IAccessToken) OAuthProvider.Current.TokenStore.Get(context.Parameters.Token, TokenType.Access)) == null)
				throw new OAuthRequestException(null, OAuthProblemTypes.TokenRejected);

			/*
			 * Ensure the token was issued to the same consumer as this request purports
			 * to be from.
			 */
			if (!accessToken.ConsumerKey.Equals(context.Parameters.ConsumerKey))
				throw new OAuthRequestException(null, OAuthProblemTypes.TokenRejected);

			switch (accessToken.Status) {
				case TokenStatus.Authorized:
					context.AccessToken = accessToken;
					break;

				case TokenStatus.Expired:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenExpired);
				case TokenStatus.Used:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenUsed);
				case TokenStatus.Revoked:
					throw new  OAuthRequestException(null, OAuthProblemTypes.TokenRevoked);
				case TokenStatus.Unauthorized:
				case TokenStatus.Unknown:
				default:
					throw new OAuthRequestException(null, OAuthProblemTypes.TokenRejected);
			}
		}

		private static void SetConsumer(OAuthRequestContext context) {
			IConsumer consumer = OAuthProvider.Current.ConsumerStore.Get(context.Parameters.ConsumerKey);
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

		private static void SetRequestId(OAuthRequestContext context) {
			long timestamp = Int64.Parse(context.Parameters.Timestamp);
			context.RequestId = OAuthProvider.Current.RequestIdValidator.ValidateRequest(context.Parameters.Nonce, timestamp,
																   context.Parameters.ConsumerKey, context.Parameters.Token);
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

		private void UpdateAccessToken(IHttpContext httpContext, OAuthRequestContext context) {
			// TODO: Update access token according to its usage policy...
		}

		private static void CopyParameters(NameValueCollection source, IDictionary<string, object> dest) {
			foreach (string parameter in source.AllKeys) {
				string[] values = source.GetValues(parameter);
				if (values == null)
					continue;

				for (int i = 0; i < values.Length; i++) {
					dest.Add(parameter, values[i]);
				}
			}
		}

		public void Configure(ConfigSource config) {
			consumerRequests = config.GetBoolean("consumerRequests");

			ConfigSource child = config.GetChild("accessVerifier");
			if (child != null) {
				string typeString = child.GetString("type");
				if (String.IsNullOrEmpty(typeString))
					throw new ConfigurationException("The 'accessVerifier' type was not specified.", child, "type");

				Type type = Type.GetType(typeString, false, true);
				if (type == null)
					throw new ConfigurationException("The type '" + typeString + "' could not be found.", child, "type");

				if (!typeof (IPathAccessVerifier).IsAssignableFrom(type))
					throw new ConfigurationException(
						"The type '" + type + "' cannot be is not an implementation of '" + typeof (IPathAccessVerifier) + "'.", child,
						"type");

				try {
					accessVerifier = (IPathAccessVerifier) Activator.CreateInstance(type);
					accessVerifier.Context = OAuthProvider.Current;
					accessVerifier.Configure(child);
				} catch (ConfigurationException) {
					throw;
				} catch (Exception e) {
					throw new ConfigurationException("Error while initializing the type '" + type + "': " + e.Message, e);
				}
			}
		}

		public AuthResult Authenticate(AuthRequest authRequest) {
			OAuthRequestContext context = new OAuthRequestContext();

			IHttpContext httpContext = HttpContextWrapper.Wrap(authRequest.Context);

			try {
				ParseParameters(httpContext, context);
				SetConsumer(context);
				SetAccessToken(context);
				context.IsOAuthRequest = true;
			} catch (OAuthRequestException ex) {
				// The request may not be an OAuth request so don't pass the exception to the consumer
				context.AddError(ex);
				context.IsOAuthRequest = false;

				AuthResult error = new AuthResult(false, ex.Code, ex.Message);
				CopyParameters(context.ResponseParameters, error.OutputData);
				return error;
			}

			try {
				SetSignProvider(context);
				SetRequestId(context);
				SetSignature(httpContext, context);
			} catch (OAuthRequestException ex) {
				context.AddError(ex);

				AuthResult error = new AuthResult(false, ex.Code, ex.Message);
				CopyParameters(context.ResponseParameters, error.OutputData);
				return error;
			}

			UpdateAccessToken(httpContext, context);

			bool canAccess;

			try {
				canAccess = VerifyAccess(authRequest.PathName, httpContext, context);
			} catch (AuthenticationException ex) {
				AuthResult error = new AuthResult(false, ex.Code, ex.Message);
				CopyParameters(context.ResponseParameters, error.OutputData);
				return error;
			}

			AuthResult result = new AuthResult(canAccess);
			CopyParameters(context.ResponseParameters, result.OutputData);
			return result;
		}

		protected virtual bool VerifyAccess(string pathName, IHttpContext httpContext, OAuthRequestContext requestContext) {
			if (accessVerifier == null)
				return true;

			return accessVerifier.CanAccess(pathName, httpContext, requestContext);
		}
	}
}