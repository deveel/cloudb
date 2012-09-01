using System;
using System.Collections.Generic;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public abstract class ServiceMessageQueue {
		protected readonly List<IServiceAddress> ServiceAddresses;
		protected readonly List<MessageStream> Messages;
		protected readonly List<ServiceType> Types;


		protected ServiceMessageQueue() {

			ServiceAddresses = new List<IServiceAddress>(4);
			Messages = new List<MessageStream>(4);
			Types = new List<ServiceType>(4);

		}

		public void AddMessageStream(IServiceAddress service_address, MessageStream message_stream, ServiceType message_type) {
			ServiceAddresses.Add(service_address);
			Messages.Add(message_stream);
			Types.Add(message_type);
		}

		public abstract void Enqueue();
	}
}