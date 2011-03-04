using System;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthClientContext {
		private INonceGenerator nonceGenerator;
		private IRequestStateStore stateStore;
		private OAuthService service;

		public OAuthClientContext(OAuthService service) {
			this.service = service;
		}

		public OAuthClientContext()
			: this(null) {
		}

		public INonceGenerator NonceGenerator {
			get { return nonceGenerator; }
			set { nonceGenerator = value; }
		}

		public IRequestStateStore RequestStateStore {
			get { return stateStore; }
			set { stateStore = value; }
		}

		public OAuthService Service {
			get { return service; }
			set { service = value; }
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, Uri callbackUri, string verifier, string endUserId) {
			OAuthRequest request = new OAuthRequest(this, resourceEndPoint, verifier, new RequestStateKey(Service, endUserId));

			request.CallbackUrl = callbackUri;

			return request;
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint) {
			return CreateRequest(resourceEndPoint, null as IToken, null as IToken);
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, IToken requestToken) {
			return CreateRequest(resourceEndPoint, requestToken, null);
		}


		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, IToken requestToken, IToken accessToken) {
			return CreateRequest(resourceEndPoint, null, requestToken, null, accessToken);
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, Uri callbackUri, IToken requestToken, IToken accessToken) {
			return CreateRequest(resourceEndPoint, callbackUri, requestToken, null, accessToken);
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, Uri callbackUri, IToken requestToken, string verifier, IToken accessToken) {
			RequestState state = new RequestState(new RequestStateKey(Service, null));
			state.RequestToken = requestToken;
			state.AccessToken = accessToken;

			OAuthRequest request = new OAuthRequest(this, resourceEndPoint, verifier, state);
			request.CallbackUrl = callbackUri;
			return request;
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, string endUserId) {
			return CreateRequest(resourceEndPoint, null, null, endUserId);
		}

		public OAuthRequest CreateRequest(OAuthEndPoint resourceEndPoint, Uri callbackUri, string endUserId) {
			return CreateRequest(resourceEndPoint, callbackUri, null, endUserId);
		}

		public OAuthRequest CreateConsumerRequest(OAuthEndPoint resourceEndPoint) {
			IToken requestToken = null, accessToken = null;
			if (service != null)
				requestToken = new EmptyToken(service.Consumer.Key, TokenType.Request);
			if (service != null)
				accessToken = new EmptyToken(service.Consumer.Key, TokenType.Access);

			return CreateRequest(resourceEndPoint, requestToken, accessToken);
		}

		public ISignProvider GetSignProvider(string signatureMethod) {
			//TODO: make it configurable ...
			return SignProviders.GetProvider(signatureMethod);
		}
	}
}