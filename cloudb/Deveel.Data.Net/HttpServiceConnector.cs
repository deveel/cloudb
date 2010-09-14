using System;

namespace Deveel.Data.Net {
	public sealed class HttpServiceConnector : IServiceConnector {
		public void Dispose() {
		}

		public void Close() {
		}

		public IMessageProcessor Connect(IServiceAddress address, ServiceType type) {
			throw new NotImplementedException();
		}
	}
}