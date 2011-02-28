using System;

namespace Deveel.Data.Net.Security {
	public interface IConsumer {
		string Key { get; }

		string Secret { get; }

		ConsumerStatus Status { get; }


		void ChangeStatus(ConsumerStatus newStatus);
	}
}