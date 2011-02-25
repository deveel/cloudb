using System;
using System.Collections.Generic;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public abstract class ServiceMessageQueue {
		private readonly List<EnqueuedMessage> messages;


		protected ServiceMessageQueue() {
			messages = new List<EnqueuedMessage>();
		}

		protected List<EnqueuedMessage> Messages {
			get { return messages; }
		}

		public void AddMessage(IServiceAddress serviceAddress, ServiceType type, Message message) {
			messages.Add(new EnqueuedMessage(message, serviceAddress, type));
		}

		public abstract void Enqueue();
	}
}