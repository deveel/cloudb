using System;

namespace Deveel.Data.Net.Security {
	public interface IConsumerStore {
		bool Add(IConsumer consumer);

		IConsumer Get(string consumerKey);

		IConsumer Remove(string consumerKey);

		bool Update(IConsumer consumer);
	}
}