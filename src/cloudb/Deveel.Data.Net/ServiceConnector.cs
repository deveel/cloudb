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
		private IAuthenticator authenticator;

		private bool connected;
		private IMessageProcessor processor;

		private Logger logger;

		private static readonly Dictionary<Type, IMessageSerializer> DefaultSerializers;

		protected ServiceConnector() {
			logger = Logger.Network;
		}

		static ServiceConnector() {
			DefaultSerializers = GetDefaultMessageSerializers();
		}

		public IMessageSerializer MessageSerializer {
			get { return serializer ?? (serializer = GetDefaultMessageSerializer(GetType())); }
			set {
				if (serializer == null)
					throw new ArgumentNullException("value");

				serializer = value;
			}
		}

		public IAuthenticator Authenticator {
			get { return authenticator; }
			set { authenticator = value; }
		}

		protected Logger Logger {
			get { return logger; }
		}

		public virtual void Close() {
		}

		void IServiceConnector.Close() {
			try {
				Close();
			} finally {
				connected = false;
				processor = null;
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Close();
			}

			base.Dispose(disposing);
		}

		IMessageProcessor IServiceConnector.Connect(IServiceAddress address, ServiceType type) {
			if (!connected) {
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

				connected = true;

				if (processor == null) {
					logger.Error(this, "It was not possible to obtain a valid message processor for the connection.");

					connected = false;
					throw new InvalidOperationException("Was not able to connect.");
				}

				OnConnected(address, type);

				logger.Info(this, "Connected to '" + address + "'.");
			}

			return processor;
		}

		protected virtual bool OnConnect(IServiceAddress address, ServiceType serviceType) {
			return true;
		}

		protected virtual void OnConnected(IServiceAddress address, ServiceType serviceType) {
		}

		protected abstract IMessageProcessor Connect(IServiceAddress address, ServiceType type);

		private static Dictionary<Type, IMessageSerializer> GetDefaultMessageSerializers() {
			throw new NotImplementedException();
		}

		private static IMessageSerializer GetDefaultMessageSerializer(Type connectorType) {
			IMessageSerializer serializer;
			if (DefaultSerializers.TryGetValue(connectorType, out serializer))
				return serializer;

			return null;
		}
	}
}