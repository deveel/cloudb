using System;

namespace Deveel.Data.Net.Security {
	public interface IConsumerDataSource {
		IConsumer GetConsumer(string consumerKey);
	}
}