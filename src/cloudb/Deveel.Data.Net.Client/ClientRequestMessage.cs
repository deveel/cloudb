using System;

namespace Deveel.Data.Net.Client {
	public sealed class ClientRequestMessage : RequestMessage {
		private readonly string clientType;
		private readonly RequestType type;
		private readonly IPathTransaction transaction;

		internal ClientRequestMessage(string clientType, RequestType type, IPathTransaction transaction) {
			this.clientType = clientType;
			this.type = type;
			this.transaction = transaction;
		}

		public string ClientType {
			get { return clientType; }
		}

		public bool IsRestClient {
			get { return String.Compare(clientType, "rest", true) == 0; }
		}

		public bool IsRpcClient {
			get { return String.Compare(clientType, "rpc", true) == 0; }
		}

		public RequestType RequestType {
			get { return type; }
		}

		public IPathTransaction Transaction {
			get { return transaction; }
		}

		internal override RequestMessage CreateClone() {
			return new ClientRequestMessage(clientType, type, transaction);
		}
	}
}