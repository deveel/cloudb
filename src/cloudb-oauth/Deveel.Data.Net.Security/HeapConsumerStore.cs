using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class HeapConsumerStore : IConsumerStore {
		private readonly Dictionary<string, IConsumer> consumers;

		private readonly object SyncRoot = new object();

		public HeapConsumerStore() {
			consumers = new Dictionary<string, IConsumer>();
		}

		public bool Add(IConsumer consumer) {
			if (consumer == null)
				throw new ArgumentNullException("consumer");

			lock (SyncRoot) {
				if (consumers.ContainsKey(consumer.Key))
					return false;

				consumers.Add(consumer.Key, consumer);
				return true;
			}
		}

		public IConsumer Get(string consumerKey) {
			if (String.IsNullOrEmpty(consumerKey))
				throw new ArgumentNullException("consumerKey");

			lock (SyncRoot) {
				IConsumer consumer;
				if (consumers.TryGetValue(consumerKey, out consumer))
					return consumer;
				return null;
			}
		}

		public IConsumer Remove(string consumerKey) {
			lock (SyncRoot) {
				IConsumer consumer;
				if (consumers.TryGetValue(consumerKey, out consumer)) {
					consumers.Remove(consumerKey);
					return consumer;
				}

				return null;
			}
		}

		public bool Update(IConsumer consumer) {
			if (consumer == null)
				throw new ArgumentNullException("consumer");

			lock (SyncRoot) {
				if (!consumers.ContainsKey(consumer.Key))
					return false;

				consumers[consumer.Key] = consumer;
				return true;
			}
		}
	}
}