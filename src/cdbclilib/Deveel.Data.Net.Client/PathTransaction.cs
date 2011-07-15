using System;

namespace Deveel.Data.Net.Client {
	public sealed class PathTransaction : IPathTransaction {
		private readonly PathClient client;

		internal PathTransaction(PathClient client) {
			this.client = client;
		}

		public void Dispose() {
			throw new NotImplementedException();
		}

		IPathClient IPathTransaction.Client {
			get { return client; }
		}

		public IPathRequest CreateRequest() {
			throw new NotImplementedException();
		}

		public void Commit() {
			throw new NotImplementedException();
		}
	}
}