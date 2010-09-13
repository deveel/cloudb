using System;

namespace Deveel.Data.Net {
	public delegate MessageStream ProcessCallback(ServiceType serviceType, MessageStream inputStream);

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

			public MessageStream Process(MessageStream messageStream) {
				return connector.callback(serviceType, messageStream);
			}
		}

		#endregion
	}
}