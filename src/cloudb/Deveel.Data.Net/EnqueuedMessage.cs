using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class EnqueuedMessage {
		private readonly IServiceAddress serviceAddress;
		private readonly Message message;
		private readonly ServiceType serviceType;

		internal EnqueuedMessage(Message message, IServiceAddress serviceAddress, ServiceType serviceType) {
			this.message = message;
			this.serviceType = serviceType;
			this.serviceAddress = serviceAddress;
		}

		public ServiceType ServiceType {
			get { return serviceType; }
		}

		public Message Message {
			get { return message; }
		}

		public IServiceAddress ServiceAddress {
			get { return serviceAddress; }
		}
	}
}