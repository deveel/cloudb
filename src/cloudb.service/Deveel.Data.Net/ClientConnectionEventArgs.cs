//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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