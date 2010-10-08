using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public delegate ResponseMessage ProcessCallback(ServiceType serviceType, RequestMessage inputStream);

	public sealed class FakeServiceConnector : IServiceConnector {
		public FakeServiceConnector(ProcessCallback callback) {
			this.callback = callback;
		}

		public FakeServiceConnector(FakeAdminService adminService) {
			callback = adminService.ProcessCallback;
		}

		private readonly ProcessCallback callback;

		public void Dispose() {
		}

		public void Close() {
		}

		public IMessageProcessor Connect(IServiceAddress address, ServiceType type) {
			return new MessageProcessor(this, type);
		}

		#region MessageProcessor

		private class MessageProcessor : IMessageProcessor {
			private readonly ServiceType serviceType;
			private readonly FakeServiceConnector connector;

			public MessageProcessor(FakeServiceConnector connector, ServiceType serviceType) {
				this.connector = connector;
				this.serviceType = serviceType;
			}

			public ResponseMessage Process(RequestMessage messageStream) {
				return connector.callback(serviceType, messageStream);
			}
		}

		#endregion
	}
}