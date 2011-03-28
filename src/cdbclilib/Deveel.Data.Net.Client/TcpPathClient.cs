using System;


namespace Deveel.Data.Net.Client {
	public sealed class TcpPathClient : PathClient {
		public TcpPathClient(string pathName) 
			: base(pathName) {
		}

		protected override void OpenConnection() {
			throw new NotImplementedException();
		}

		protected override void CloseConnection() {
			throw new NotImplementedException();
		}

		protected override IPathTransaction CreateTransaction() {
			throw new NotImplementedException();
		}
	}
}
