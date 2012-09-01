using System;
using System.Collections.Generic;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public delegate IEnumerable<Message> ProcessCallback(ServiceType serviceType, IEnumerable<Message> inputStream);

	public sealed class FakeServiceConnector : IServiceConnector {
		public FakeServiceConnector(ProcessCallback callback) {
			this.callback = callback;
		}

		public FakeServiceConnector(FakeAdminService adminService) {
			callback = adminService.ProcessCallback;
		}

		private readonly ProcessCallback callback;
		private IMessageSerializer serializer;
		private IServiceAuthenticator authenticator;

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

		public IServiceAuthenticator Authenticator {
			get { return authenticator; }
			set { authenticator = value; }
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

			public IEnumerable<Message> Process(IEnumerable<Message> messageStream) {
				return connector.callback(serviceType, messageStream);
			}
		}

		#endregion
	}
}