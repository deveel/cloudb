using System;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthRequest {
		private readonly OAuthClientContext context;
		private readonly RequestState state;
		private readonly OAuthEndPoint resourceEndPoint;
		private string verifier;
		private Uri callbackUrl;
		private IVerifierCollector verifierCollector;


		private const string HttpPostUrlEncodedContentType = "application/x-www-form-urlencoded";
		private static readonly Regex HttpPostUrlEncodedContentTypeRegex = new Regex(@"^application/x-www-form-urlencoded;?", RegexOptions.Compiled);
		private IAuthorizationHandler authorizationHandler;
		private const string OAuthOutOfBandCallback = "oob";
		
		public event EventHandler BeforeGetProtectedResource;
		public event EventHandler ReceiveRequestToken;
		public event EventHandler BeforeGetRequestToken;
		public event EventHandler BeforeGetAccessToken;
		public event EventHandler ReceiveAccessToken;

		internal OAuthRequest(OAuthClientContext context, OAuthEndPoint resourceEndPoint, string verifier, RequestState state) {
			this.context = context;
			this.resourceEndPoint = resourceEndPoint;
			this.verifier = verifier;

			this.state = state;
		}

		internal OAuthRequest(OAuthClientContext context, OAuthEndPoint resourceEndPoint, string verifier, RequestStateKey stateKey) {
			this.context = context;
			this.resourceEndPoint = resourceEndPoint;
			this.verifier = verifier;
			state = context.RequestStateStore.Get(stateKey);
		}

		public OAuthClientContext ClientContext {
			get { return context; }
		}

		public OAuthEndPoint ResourceEndPoint {
			get { return resourceEndPoint; }
		}

		public Uri ResourceUri {
			get { return resourceEndPoint.Uri; }
		}

		public OAuthService Service {
			get { return context.Service; }
		}

		public IToken RequestToken {
			get { return state.RequestToken; }
			private set {
				state.RequestToken = value;

				if (context.RequestStateStore != null)
					context.RequestStateStore.Store(state);
			}
		}

		public IToken AccessToken {
			get { return state.AccessToken; }
			private set {
				state.AccessToken = value;

				if (context.RequestStateStore != null)
					context.RequestStateStore.Store(state);
			}
		}

		public Uri CallbackUrl {
			get { return callbackUrl; }
			internal set { callbackUrl = value; }
		}

		public string RequestTokenVerifier {
			get { return verifier; }
		}

		public IVerifierCollector VerifierCollector {
			get { return verifierCollector; }
			set { verifierCollector = value; }
		}

		public IAuthorizationHandler AuthorizationHandler {
			get { return authorizationHandler; }
			set { authorizationHandler = value; }
		}

		public OAuthResponse GetResource() {
			return GetResource(null);
		}

		public OAuthResponse GetResource(NameValueCollection parameters) {
			return GetResource(parameters,
			                   ResourceEndPoint.HttpMethod == "POST" || this.ResourceEndPoint.HttpMethod == "PUT"
			                   	? HttpPostUrlEncodedContentType
			                   	: String.Empty, null);
		}

		public OAuthResponse GetResource(string contentType, Stream bodyStream) {
			////The contentType must be supplied but bodyBytes can be null or 0 length.
			if (String.IsNullOrEmpty(contentType))
				throw new ArgumentException("Cannot be null or empty", "contentType");

			if (HttpPostUrlEncodedContentTypeRegex.IsMatch(contentType))
				throw new ArgumentException(
					String.Format(
						"Invalid method call.  Use GetResource(NameValueCollection parameters) for HTTP requests of content-type {0}.",
						HttpPostUrlEncodedContentType), "contentType");

			////Check to see if we are a GET or DELETE can't send a body in a GET or DELETE
			if (ResourceEndPoint.HttpMethod == "GET" || ResourceEndPoint.HttpMethod == "DELETE")
				throw new InvalidOperationException(
					"You cannot send an entity in the HTTP request in a GET or DELETE HttpMethod.");

			return GetResource(null, contentType, bodyStream);
		}

		private HttpWebRequest PrepareProtectedResourceRequest(NameValueCollection parameters, string contentType, Stream bodyStream) {
			if (AccessToken == null) {
				if (RequestToken == null) {
					// Get a request token
					DoGetRequestToken();

					if (RequestToken == null)
						throw new InvalidOperationException("Request token was not received.");

					// Get the authorization handler to authorize the request token
					// Halt processing if the authorization handler is out-of-band
					if (!DoAuthorizeRequestToken())
						return null;
				}

				if (String.IsNullOrEmpty(RequestTokenVerifier)) {
					// Try to collect the verifier
					DoCollectVerifier();
				}

				if (String.IsNullOrEmpty(RequestTokenVerifier))
					return null;

				// Get the access token - this will return false if the verifier is not provided
				// the implementation needs to get the user to re-authenticate.
				if (!DoGetAccessToken())
					return null;
			}

			if (AccessToken == null)
				throw new InvalidOperationException("Access token was not received.");

			return DoPrepareProtectedResourceRequest(parameters, contentType, bodyStream);
		}

		private void DoGetRequestToken() {
			// Fire the OnBeforeGetRequestToken event
			PreRequestEventArgs args = new PreRequestEventArgs(Service.RequestTokenUrl, Service.RequestTokenEndPoint.HttpMethod, CallbackUrl);

			if (BeforeGetRequestToken != null)
				BeforeGetRequestToken(this, args);

			OAuthParameters authParams = CreateOAuthParameters(args.AdditionalParameters);
			authParams.Callback = args.CallbackUrl == null ? OAuthOutOfBandCallback : args.CallbackUrl.AbsoluteUri;

			SignParameters(args.RequestUri, args.HttpMethod, authParams, null);

			// Create and sign the request
			HttpWebRequest request = CreateRequest(
				args.RequestUri,
				authParams,
				args.HttpMethod,
				args.HttpMethod == "POST" ? HttpPostUrlEncodedContentType : String.Empty,
				null);

			OAuthParameters responseParameters;

			// Get the service provider response
			try {
				HttpWebResponse response = (HttpWebResponse)request.GetResponse();

				// Parse the parameters and re-throw any OAuthRequestException from the service provider
				responseParameters = OAuthParameters.Parse(response);
				OAuthRequestException.TryRethrow(responseParameters);
			} catch (WebException e) {
				// Parse the parameters and re-throw any OAuthRequestException from the service provider
				responseParameters = OAuthParameters.Parse(e.Response as HttpWebResponse);
				OAuthRequestException.TryRethrow(responseParameters);

				// If no OAuthRequestException, rethrow the WebException
				throw;
			}

			// Store the request token
			RequestToken = new OAuthToken(TokenType.Request, responseParameters.Token, responseParameters.TokenSecret, Service.Consumer);

			// Fire the OnReceiveRequestToken event
			RequestTokenReceivedEventArgs responseArgs = new RequestTokenReceivedEventArgs(RequestToken);
			responseArgs.Parameters.Add(responseParameters.AdditionalParameters);

			if (ReceiveRequestToken != null)
				ReceiveRequestToken(this, responseArgs);
		}

		private bool DoAuthorizeRequestToken() {
			if (RequestToken == null)
				throw new InvalidOperationException("Request token must be present");

			// Invoke the authorization handler
			bool continueOnReturn = false;
			if (authorizationHandler != null)
				continueOnReturn = authorizationHandler.Authorize(RequestToken);

			return continueOnReturn;
		}

		private void DoCollectVerifier() {
			// Invoke the authorization handler
			string v = null;
			if (verifierCollector != null)
				v = verifierCollector.CollectVerifier();

			// Store the verifier if it has been specified
			if (!String.IsNullOrEmpty(v))
				verifier = v;
		}

		private bool DoGetAccessToken() {
			// Fire the OnBeforeGetAccessToken event
			PreAccessTokenRequestEventArgs preArgs = new PreAccessTokenRequestEventArgs(Service.AccessTokenUrl,
			                                                                            Service.AccessTokenEndPoint.HttpMethod,
			                                                                            RequestToken, RequestTokenVerifier);
			if (BeforeGetAccessToken != null)
				BeforeGetAccessToken(this, preArgs);

			// Create and sign the request
			OAuthParameters authParams = CreateOAuthParameters(null);
			authParams.Verifier = preArgs.Verifier;

			// We don't have a verifier so something has gone wrong in the process.                
			if (string.IsNullOrEmpty(authParams.Verifier))
				return false;

			SignParameters(preArgs.RequestUri, preArgs.HttpMethod, authParams, RequestToken);

			HttpWebRequest request = CreateRequest(preArgs.RequestUri, authParams, preArgs.HttpMethod,
			                                       preArgs.HttpMethod == "POST" ? HttpPostUrlEncodedContentType : String.Empty,
			                                       null);

			OAuthParameters responseParameters;

			// Get the service provider response
			try {
				HttpWebResponse response = (HttpWebResponse)request.GetResponse();

				// Parse the parameters and re-throw any OAuthRequestException from the service provider
				responseParameters = OAuthParameters.Parse(response);
				OAuthRequestException.TryRethrow(responseParameters);
			} catch (WebException e) {
				// Parse the parameters and re-throw any OAuthRequestException from the service provider
				responseParameters = OAuthParameters.Parse(e.Response as HttpWebResponse);
				OAuthRequestException.TryRethrow(responseParameters);

				// If no OAuthRequestException, rethrow the WebException
				throw;
			}

			// Store the access token
			AccessToken = new OAuthToken(TokenType.Access, responseParameters.Token, responseParameters.TokenSecret, Service.Consumer);

			// Fire the OnReceiveAccessToken event
			AccessTokenReceivedEventArgs responseArgs = new AccessTokenReceivedEventArgs(RequestToken, AccessToken);
			responseArgs.AdditionalParameters.Add(responseParameters.AdditionalParameters);

			if (ReceiveAccessToken != null)
				ReceiveAccessToken(this, responseArgs);

			return true;
		}

		private HttpWebRequest DoPrepareProtectedResourceRequest(NameValueCollection parameters, string contentType, Stream bodyStream) {
			// Fire the OnBeforeGetProtectedResource event
			PreProtectedResourceRequestEventArgs preArgs = new PreProtectedResourceRequestEventArgs(ResourceUri,
			                                                                                        ResourceEndPoint.HttpMethod,
			                                                                                        RequestToken, AccessToken);

			if (parameters !=null)
				preArgs.AdditionalParameters.Add(parameters);

			if (BeforeGetProtectedResource != null)
				BeforeGetProtectedResource(this, preArgs);

			OAuthParameters authParams = CreateOAuthParameters(preArgs.AdditionalParameters);

			SignParameters(preArgs.RequestUri, preArgs.HttpMethod, authParams, AccessToken);

			return CreateRequest(preArgs.RequestUri, authParams, preArgs.HttpMethod, contentType, bodyStream);
		}

		private void SignParameters(Uri requestUri, string httpMethod, OAuthParameters authParameters, IToken token) {
			// Check there is a signing provider for the signature method
			ISignProvider signingProvider = context.GetSignProvider(Service.SignatureMethod);

			if (signingProvider == null)
				// There is no signing provider for this signature method
				throw new OAuthRequestException(null, OAuthProblemTypes.SignatureMethodRejected);

			// Double check the signing provider declares that it can handle the signature method
			if (!signingProvider.SignatureMethod.Equals(Service.SignatureMethod))
				throw new OAuthRequestException(null, OAuthProblemTypes.SignatureMethodRejected);

			// Compute the signature
			authParameters.Sign(requestUri, httpMethod, Service.Consumer, token, signingProvider);
		}

		private HttpWebRequest CreateRequest(Uri requestUri, OAuthParameters authParameters, string httpMethod, string contentType, Stream bodyStream) {
			NameValueCollection requestSpecificParameters = new NameValueCollection(authParameters.AdditionalParameters);
			if (!Service.UseAuthorizationHeader) {
				////The OAuth params need to be added either into the querystring or into the post body.
				requestSpecificParameters.Add(authParameters.OAuthRequestParams());
			}

			if (HttpPostUrlEncodedContentTypeRegex.IsMatch(contentType) && bodyStream == null) {
				////All the requestSpecificParameters need to be encoded into the body bytes
				string body = Rfc3986.EncodeAndJoin(requestSpecificParameters);
				bodyStream = new MemoryStream(Encoding.ASCII.GetBytes(body));
			} else {
				////They go into the querystring.
				string query = Rfc3986.EncodeAndJoin(requestSpecificParameters);

				if (!string.IsNullOrEmpty(query)) {
					UriBuilder mutableRequestUri = new UriBuilder(requestUri);
					if (string.IsNullOrEmpty(mutableRequestUri.Query))
						mutableRequestUri.Query = query;
					else
						mutableRequestUri.Query = mutableRequestUri.Query.Substring(1) + "&" + query;

					requestUri = mutableRequestUri.Uri;
				}
			}

			HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(requestUri);
			request.Method = httpMethod;

			if (Service.UseAuthorizationHeader)
				request.Headers.Add(HttpRequestHeader.Authorization, authParameters.ToHeader());

			if (!String.IsNullOrEmpty(contentType)) {
				request.ContentType = contentType;

				if (bodyStream != null) {
					if (bodyStream.CanSeek)
						request.ContentLength = bodyStream.Length;

					StreamCopier.CopyTo(bodyStream, request.GetRequestStream());
				}
			}

			return request;
		}

		private OAuthResponse GetResource(NameValueCollection parameters, string contentType, Stream bodyStream) {
			OAuthResponse response;

			HttpWebRequest request = PrepareProtectedResourceRequest(parameters, contentType, bodyStream);

			// A null value for the HttpWebRequest is returned when a ResponseToken is returned
			// and no one has returned in the AuthorizationHandler continue with getting an AccessToken
			// or an RequestToken exists but the AccessToken request was refused.
			if (request == null)
				response = new OAuthResponse(RequestToken);
			else {
				OAuthParameters responseParameters;

				try {
					OAuthResource resource = new OAuthResource((HttpWebResponse)request.GetResponse());

					// Parse the parameters and re-throw any OAuthRequestException from the service provider
					responseParameters = OAuthParameters.Parse(resource);
					OAuthRequestException.TryRethrow(responseParameters);

					// If nothing is thrown then we should have a valid resource.
					response = new OAuthResponse(AccessToken ?? RequestToken, resource);
				} catch (WebException e) {
					// Parse the parameters and re-throw any OAuthRequestException from the service provider
					responseParameters = OAuthParameters.Parse(e.Response as HttpWebResponse);
					OAuthRequestException.TryRethrow(responseParameters);

					// If no OAuthRequestException, rethrow the WebException
#warning TODO: We have consumer the WebException's body so rethrowing it is pretty pointless; wrap the WebException in an OAuthProtocolException and store the body (create an OAuthResource before parsing parameters)
					throw;
				}
			}

			return response;
		}

		private OAuthParameters CreateOAuthParameters(NameValueCollection additionalParameters) {
			int timestamp = UnixTime.ToUnixTime(DateTime.Now);

			OAuthParameters authParameters = new OAuthParameters();
			authParameters.ConsumerKey = Service.Consumer.Key;
			authParameters.Realm = Service.Realm;
			authParameters.SignatureMethod = Service.SignatureMethod;
			authParameters.Timestamp = timestamp.ToString(CultureInfo.InvariantCulture);
			authParameters.Nonce = context.NonceGenerator.GenerateNonce(timestamp);
			authParameters.Version = Service.OAuthVersion;

			if (additionalParameters != null && additionalParameters.Count > 0)
				authParameters.AdditionalParameters.Add(additionalParameters);

			return authParameters;
		}
	}
}