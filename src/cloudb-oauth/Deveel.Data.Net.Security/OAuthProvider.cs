using System;
using System.Net;
using System.Web;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public abstract class OAuthProvider : IConfigurable {
		private bool allowOutOfBandCallback;
		private IRequestIdValidator requestIdValidator;
		private IConsumerStore consumerStore;
		private ITokenGenerator tokenGenerator;
		private ITokenStore tokenStore;
		private IResourceAccessVerifier resourceAccessVerifier;
		private IVerificationProvider verificationProvider;
		private ICallbackStore callbackStore;
		private ConfigSource configSource;
		private readonly OAuthRequestTokenIssuer requestTokenIssuer;
		private readonly OAuthAccessTokenIssuer accessTokenIssuer;

		private static OAuthProvider current;

		protected OAuthProvider() {
			requestTokenIssuer = new OAuthRequestTokenIssuer(this);
			accessTokenIssuer = new OAuthAccessTokenIssuer(this);
		}

		public IConsumerStore ConsumerStore {
			get { return consumerStore; }
			set { consumerStore = value; }
		}

		public ICallbackStore CallbackStore {
			get { return callbackStore; }
			set { callbackStore = value; }
		}

		public ITokenStore TokenStore {
			get { return tokenStore; }
			set { tokenStore = value; }
		}

		public ITokenGenerator TokenGenerator {
			get { return tokenGenerator; }
			set { tokenGenerator = value; }
		}

		public IRequestIdValidator RequestIdValidator {
			get { return requestIdValidator; }
			set { requestIdValidator = value; }
		}

		public IVerificationProvider VerificationProvider {
			get { return verificationProvider; }
			set { verificationProvider = value; }
		}

		public IResourceAccessVerifier ResourceAccessVerifier {
			get { return resourceAccessVerifier; }
			set { resourceAccessVerifier = value; }
		}

		protected ConfigSource Config {
			get { return configSource; }
		}

		public bool AllowOutOfBandCallback {
			get { return allowOutOfBandCallback; }
			set { allowOutOfBandCallback = value; }
		}

		public static OAuthProvider Current {
			get { return current; }
		}

		private object ConfigureComponent(ConfigSource config, Type componentType, Type defaultType) {
			object obj = null;
			if (config != null) {
				string typeString = config.GetString("type");
				if (String.IsNullOrEmpty(typeString)) {
					if (defaultType != null)
						obj = Activator.CreateInstance(defaultType);
				} else {
					Type type = Type.GetType(typeString, false, true);
					if (type == null)
						throw new ConfigurationException("The type '" + typeString + "' was not found in the current context.", config, "type");

					if (!componentType.IsAssignableFrom(type))
						throw new ConfigurationException("The type '" + type + "' is not an instance of '" + typeof(ITokenStore) + "'.");

					try {
						obj = Activator.CreateInstance(type, null);
					} catch (Exception e) {
						throw new ConfigurationException("Unable to instantiate the type '" + type + "': " + e.Message, config, "type");
					}
				}
			} else {
				if (defaultType != null)
					obj = Activator.CreateInstance(defaultType);
			}

			if (obj == null)
				return null;

			if (obj is IRequiresProviderContext)
				((IRequiresProviderContext)obj).Context = this;
			if (obj is IConfigurable)
				((IConfigurable)obj).Configure(config);

			return obj;
		}

		protected TokenIssueResult IssueAccessToken(IHttpContext context) {
			return accessTokenIssuer.ProcessIssueRequest(context);
		}

		protected TokenIssueResult IssueRequestToken(IHttpContext context) {
			return requestTokenIssuer.ProcessIssueRequest(context);
		}

		protected virtual void Configure(ConfigSource config) {
			tokenStore = (ITokenStore)ConfigureComponent(config.GetChild("tokenStore"), typeof(ITokenStore), typeof(HeapTokenStore));
			callbackStore = (ICallbackStore)ConfigureComponent(config.GetChild("callbackStore"), typeof(ICallbackStore), typeof(HeapCallbackStore));
			consumerStore = (IConsumerStore)ConfigureComponent(config.GetChild("consumerStore"), typeof(IConsumerStore), typeof(HeapConsumerStore));
			requestIdValidator = (IRequestIdValidator)ConfigureComponent(config.GetChild("requestIdValidator"), typeof(IRequestIdValidator), typeof(HeapRequestIdValidator));
			resourceAccessVerifier = (IResourceAccessVerifier)ConfigureComponent(config.GetChild("resourceAccessVerifier"), typeof(IResourceAccessVerifier), null);
			tokenGenerator = (ITokenGenerator)ConfigureComponent(config.GetChild("tokenGenerator"), typeof(ITokenGenerator), typeof(GuidTokenGenerator));
			verificationProvider = (IVerificationProvider)ConfigureComponent(config.GetChild("verificationProvider"), typeof(IVerificationProvider), typeof(MD5HashVerificationProvider));

			allowOutOfBandCallback = config.GetBoolean("oobCallback");
		}

		void IConfigurable.Configure(ConfigSource config) {
			configSource = config;
			Configure(config);

			//TODO: find a more proper way to the set the singleton ...
			if (current == null)
				SetCurrent(this);
		}

		public virtual TokenIssueResult IssueToken(TokenType tokenType, IHttpContext context) {
			if (tokenType == TokenType.Request)
				return IssueRequestToken(context);
			if (tokenType == TokenType.Access)
				return accessTokenIssuer.ProcessIssueRequest(context);

			throw new InvalidOperationException();
		}

		public TokenIssueResult IssueToken(TokenType tokenType, HttpContext context) {
			return IssueToken(tokenType, HttpContextWrapper.Wrap(context));
		}

		public TokenIssueResult IssueToken(TokenType tokenType, HttpListenerContext context) {
			return IssueToken(tokenType, HttpContextWrapper.Wrap(context));
		}

		public static void SetCurrent(OAuthProvider provider) {
			if (provider == null)
				throw new ArgumentNullException("provider");

			if (current != null)
				throw new InvalidOperationException();

			current = provider;
		}
	}
}