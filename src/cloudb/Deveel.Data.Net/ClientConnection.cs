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
using System.Collections.Generic;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public sealed class ClientConnection {
		private readonly string remoteEndPoint;
		private readonly AdminService service;
		private readonly string protocol;
		private DateTime started;
		private DateTime ended;
		private bool connected;

		private IEnumerable<Message> currentRequest;
		private DateTime currentRequestTime;
		private ServiceType currentServiceType;

		private int commandCount;

		internal ClientConnection(AdminService service, string protocol, string remoteEndPoint) {
			this.service = service;
			this.remoteEndPoint = remoteEndPoint;
			this.protocol = protocol;
		}

		public DateTime Started {
			get { return started; }
		}

		public DateTime Ended {
			get { return ended; }
		}

		public string Protocol {
			get { return protocol; }
		}

		public AdminService Service {
			get { return service; }
		}

		public bool IsConnected {
			get { return connected; }
		}

		public string LocalEndPoint {
			get { return service.Address.ToString(); }
		}

		public string RemoteEndPoint {
			get { return remoteEndPoint; }
		}

		public bool HasRequestPending {
			get { return currentRequest != null; }
		}

		public TimeSpan LifeTime {
			get { return ended - started; }
		}

		public int CommandCount {
			get { return commandCount; }
		}

		internal void Disconnect() {
			ClientConnectionEventArgs args = new ClientConnectionEventArgs(protocol, LocalEndPoint, remoteEndPoint, started);

			try {
				service.OnClientDisconnect(args);
			} finally {
				ended = DateTime.Now;
				connected = false;
			}
		}

		internal bool Connect(bool authorized) {
			started = DateTime.Now;

			try {
				ClientConnectionEventArgs args = new ClientConnectionEventArgs(protocol, LocalEndPoint, remoteEndPoint, authorized);
				authorized = service.OnClientConnect(args);
				connected = true;
				return authorized;
			} catch (Exception) {
				connected = false;
				return false;
			}
		}

		internal void Request(ServiceType serviceType, IEnumerable<Message> request) {
			currentRequest = request;
			currentRequestTime = DateTime.Now;
			currentServiceType = serviceType;

			ClientCommandEventArgs args = new ClientCommandEventArgs(LocalEndPoint, remoteEndPoint, currentServiceType,
			                                                         currentRequest, currentRequestTime);

			service.OnClientRequest(args);
		}

		internal void Response(IEnumerable<Message> response) {
			ClientCommandEventArgs args = new ClientCommandEventArgs(LocalEndPoint, remoteEndPoint, currentServiceType,
			                                                         currentRequest, currentRequestTime, response, DateTime.Now);

			try {
				service.OnClientResponse(args);
				//TODO: should we store the entire command?
				commandCount++;
			} finally {
				currentRequest = null;
			}
		}
	}
}