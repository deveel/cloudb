using System;
using System.Collections.Generic;
using System.ComponentModel;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;
using Deveel.Data.Net.Security;
using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net {
	public abstract class ServiceConnector : Component, IServiceConnector {
		private IMessageSerializer serializer;
		private Logger logger;

		private IAuthenticator authenticator;
		private readonly IDictionary<ServiceInfo, object> authenticatedSessions;

		public event AuthenticationEventHandler AuthenticationFailed;

		protected ServiceConnector() {
			logger = Logger.Network;

			authenticatedSessions = new Dictionary<ServiceInfo, object>();
		}

		public IMessageSerializer MessageSerializer {
			get { return serializer ?? (serializer = GetDefaultMessageSerializer()); }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				serializer = value;
			}
		}

		protected Logger Logger {
			get { return logger; }
		}

		public IAuthenticator Authenticator {
			get { return authenticator; }
			set { authenticator = value; }
		}

		public virtual void Close() {
		}

		void IServiceConnector.Close() {
			try {
				logger.Info(this, "Closing connector");

				if (authenticator != null) {
					
				}

				Close();
			} catch(Exception e) {
				logger.Error(this, "An error occurred while closing the connector", e);
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				(this as IServiceConnector).Close();
			}

			base.Dispose(disposing);
		}

		IMessageProcessor IServiceConnector.Connect(IServiceAddress address, ServiceType type) {
			IMessageProcessor processor;

			if (!OnConnect(address, type)) {
				logger.Warning(this, "Unable to connect to '" + address + "' after check.");
				return null;
			}

			try {
				processor = Connect(address, type);
			} catch (Exception e) {
				logger.Error(this, "Error while connecting.", e);
				throw;
			}

			if (processor == null) {
				logger.Error(this, "It was not possible to obtain a valid message processor for the connection.");
				throw new InvalidOperationException("Was not able to connect.");
			}

			if (authenticator != null) {
				logger.Info("Authenticating the connection.");

				IDictionary<string, AuthObject> authData = new Dictionary<string, AuthObject>();
				authenticator.CollectData(authData);
				OnAuthenticate(authData);
				AuthRequest request = new AuthRequest(this, authData);

				try {
					AuthResult result = authenticator.Authenticate(request);
					while (result.Code == (int)AuthenticationCode.NeedMoreData) {
						logger.Info("The authentication process needs more data.");

						result = authenticator.Authenticate(request);
					}

					if (result.Code != (int)AuthenticationCode.Success)
						throw new AuthenticationException(result.Message, result.Code);

					logger.Info("Successfully authenticated.");

					OnAuthenticated(result);
				} catch (AuthenticationException e) {
					logger.Warning("The authentication failed explicitely.");

					OnAuthenticationFailed(e, authData);
				} catch (Exception e) {
					logger.Warning("The authentication failed because of an unknown error", e);

					OnAuthenticationFailed(new AuthenticationException(e.Message, (int)AuthenticationCode.SystemError), authData);
				}
			}

			OnConnected(address, type);

			logger.Info(this, "Connected to '" + address + "'.");

			return processor;
		}

		protected virtual void OnAuthenticated(AuthResult authResult) {
			throw new NotImplementedException();
		}

		protected virtual bool OnConnect(IServiceAddress address, ServiceType serviceType) {
			return true;
		}

		protected virtual void OnConnected(IServiceAddress address, ServiceType serviceType) {
		}

		protected virtual void OnAuthenticate(IDictionary<string, AuthObject> authData) {
		}

		protected virtual void OnAuthenticationFailed(AuthenticationException e, IDictionary<string,AuthObject> authData) {
			if (AuthenticationFailed != null)
				AuthenticationFailed(this, new AuthenticationEventArgs(e, authData));
		}

		protected abstract IMessageProcessor Connect(IServiceAddress address, ServiceType type);

		protected virtual IMessageSerializer GetDefaultMessageSerializer() {
			object[] attrs = GetType().GetCustomAttributes(typeof (MessageSerializerAttribute), true);
			if (attrs.Length == 0)
				return null;

			MessageSerializerAttribute attribute = (MessageSerializerAttribute) attrs[0];
			return attribute.WithName
			       	? MessageSerializers.GetSerializer(attribute.SerializerName)
			       	: MessageSerializers.GetSerializer(attribute.SerializerType);
		}

		#region ServiceInfo

		private struct ServiceInfo {
			private readonly IServiceAddress address;
			private readonly ServiceType serviceType;

			public ServiceInfo(IServiceAddress address, ServiceType serviceType) 
				: this() {
				this.address = address;
				this.serviceType = serviceType;
			}

			public override int GetHashCode() {
				return address.GetHashCode() ^ serviceType.GetHashCode();
			}

			public override bool Equals(object obj) {
				ServiceInfo other = (ServiceInfo) obj;
				return address.Equals(other.address) && serviceType.Equals(other.serviceType);
			}
		}

		#endregion
	}
}