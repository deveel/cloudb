//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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