using System;

using Deveel.Data.Net.Client;
using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net {
	public delegate Message ProcessCallback(ServiceType serviceType, Message inputStream);

	public sealed class FakeServiceConnector : IServiceConnector {
		public FakeServiceConnector(ProcessCallback callback) {
			this.callback = callback;
		}

		public FakeServiceConnector(FakeAdminService adminService) {
			callback = adminService.ProcessCallback;
		}

		private readonly ProcessCallback callback;
		private IMessageSerializer serializer;

		public void Dispose() {
		}

		public IMessageSerializer MessageSerializer {
			get {
				if (serializer == null)
					serializer = new BinaryRpcMessageSerializer();
				return serializer;
			}
			set { serializer = value; }
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

			public Message Process(Message messageStream) {
				return connector.callback(serviceType, messageStream);
			}
		}

		#endregion
	}
}