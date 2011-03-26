using System;

namespace Deveel.Data.Net {
	public delegate void ClientConnectionEventHandler(object sender, ClientConnectionEventArgs args);

	public sealed class ClientConnectionEventArgs : EventArgs {
		private readonly string protocol;
		private readonly string remoteEndPoint;
		private readonly string localEndPoint;
		private bool authorized;
		private readonly DateTime connectTime;
		private readonly DateTime disconnectTime;
		private readonly bool disconnected;

		internal ClientConnectionEventArgs(string protocol, string localEndPoint, string remoteEndPoint, bool authorized) {
			this.protocol = protocol;
			this.authorized = authorized;
			this.remoteEndPoint = remoteEndPoint;
			this.localEndPoint = localEndPoint;
			connectTime = DateTime.Now;
		}

		internal ClientConnectionEventArgs(string protocol, string localEndPoint, string remoteEndPoint, DateTime connectTime) {
			this.protocol = protocol;
			this.remoteEndPoint = remoteEndPoint;
			this.localEndPoint = localEndPoint;
			this.connectTime = connectTime;
			disconnectTime = DateTime.Now;
			authorized = true;
			disconnected = true;
		}

		public DateTime DisconnectTime {
			get { return disconnectTime; }
		}

		public DateTime ConnectTime {
			get { return connectTime; }
		}

		public TimeSpan Elapsed {
			get { return disconnected ? DisconnectTime - ConnectTime : TimeSpan.Zero; }
		}

		public bool Authorized {
			get { return authorized; }
			set { authorized = value; }
		}

		public string RemoteEndPoint {
			get { return remoteEndPoint; }
		}

		public string LocalEndPoint {
			get { return localEndPoint; }
		}

		public string Protocol {
			get { return protocol; }
		}
	}
}