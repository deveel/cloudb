using System;
using System.ComponentModel;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public abstract class ServiceConnector : Component, IServiceConnector {
		private IMessageSerializer messageSerializer;
		private IServiceAuthenticator authenticator;
		private bool isClosed;
		private readonly Logger logger;

		private static readonly IServiceAuthenticator NoAuthentication = NoAuthenticationAuthenticator.Instance;

		protected ServiceConnector() {
			logger = Logger.Network;
		}

		protected Logger Logger {
			get { return logger; }
		}

		public virtual IMessageSerializer MessageSerializer {
			get { return messageSerializer; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				messageSerializer = value;
			}
		}

		public virtual IServiceAuthenticator Authenticator {
			get { return authenticator ?? NoAuthentication; }
			set { authenticator = value; }
		}

		protected bool IsClosed {
			get { return isClosed; }
		}

		protected abstract IMessageProcessor Connect(IServiceAddress address, ServiceType type);

		IMessageProcessor IServiceConnector.Connect(IServiceAddress address, ServiceType type) {
			return Connect(address, type);
		}

		public virtual void Close() {
			if (!isClosed) {
				Dispose(true);
				isClosed = true;
			}
		}
	}
}