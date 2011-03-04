using System;

namespace Deveel.Data.Net.Security.Fake {
	public sealed class FakeConsumer : IConsumer {
		private readonly string key;
		private readonly string secret;
		private ConsumerStatus status;
		private bool statusChanged;

		public FakeConsumer(string key, string secret, ConsumerStatus status) {
			this.key = key;
			this.status = status;
			this.secret = secret;
		}

		public FakeConsumer(string key, string secret)
			: this(key, secret, ConsumerStatus.Unknown) {
		}

		public bool StatusChanged {
			get { return statusChanged; }
		}

		public string Key {
			get { return key; }
		}

		public string Secret {
			get { return secret; }
		}

		public ConsumerStatus Status {
			get { return status; }
		}

		public void ChangeStatus(ConsumerStatus newStatus) {
			statusChanged = status != newStatus;
			status = newStatus;
		}
	}
}