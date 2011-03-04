using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public abstract class OAuthProvider : IOAuthProvider, IConfigurable {
		private IRequestIdValidator requestIdValidator;
		private IConsumerStore consumerStore;
		private ITokenGenerator tokenGenerator;
		private ITokenStore tokenStore;
		private IResourceAccessVerifier resourceAccessVerifier;
		private IVerificationProvider verificationProvider;
		private ICallbackStore callbackStore;

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

		private object ConfigureComponent(ConfigSource configSource, Type componentType, Type defaultType) {
			object obj = null;
			if (configSource != null) {
				string typeString = configSource.GetString("type");
				if (String.IsNullOrEmpty(typeString)) {
					if (defaultType != null)
						obj = Activator.CreateInstance(defaultType);
				} else {
					Type type = Type.GetType(typeString, false, true);
					if (type == null)
						throw new ConfigurationException("The type '" + typeString + "' was not found in the current context.", configSource, "type");

					if (!componentType.IsAssignableFrom(type))
						throw new ConfigurationException("The type '" + type + "' is not an instance of '" + typeof(ITokenStore) + "'.");

					try {
						obj = Activator.CreateInstance(type, null);
					} catch (Exception e) {
						throw new ConfigurationException("Unable to instantiate the type '" + type + "': " + e.Message, configSource, "type");
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
				((IConfigurable)obj).Configure(configSource);

			return obj;
		}

		public virtual void Configure(ConfigSource config) {
			tokenStore = (ITokenStore)ConfigureComponent(config.GetChild("tokenStore"), typeof(ITokenStore), typeof(HeapTokenStore));
			callbackStore = (ICallbackStore)ConfigureComponent(config.GetChild("callbackStore"), typeof(ICallbackStore), typeof(HeapCallbackStore));
			consumerStore = (IConsumerStore)ConfigureComponent(config.GetChild("consumerStore"), typeof(IConsumerStore), typeof(HeapConsumerStore));
			requestIdValidator = (IRequestIdValidator)ConfigureComponent(config.GetChild("requestIdValidator"), typeof(IRequestIdValidator), typeof(HeapRequestIdValidator));
			resourceAccessVerifier = (IResourceAccessVerifier)ConfigureComponent(config.GetChild("resourceAccessVerifier"), typeof(IResourceAccessVerifier), null);
			tokenGenerator = (ITokenGenerator)ConfigureComponent(config.GetChild("tokenGenerator"), typeof(ITokenGenerator), typeof(GuidTokenGenerator));
			verificationProvider = (IVerificationProvider)ConfigureComponent(config.GetChild("verificationProvider"), typeof(IVerificationProvider), typeof(MD5HashVerificationProvider));
		}
	}
}