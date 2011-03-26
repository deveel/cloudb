using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class ClientConnection {
		private readonly string remoteEndPoint;
		private readonly AdminService service;
		private readonly string protocol;
		private DateTime started;
		private DateTime ended;
		private bool connected;

		private Message currentRequest;
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

		internal void Request(ServiceType serviceType, Message request) {
			currentRequest = request;
			currentRequestTime = DateTime.Now;
			currentServiceType = serviceType;

			ClientCommandEventArgs args = new ClientCommandEventArgs(LocalEndPoint, remoteEndPoint, currentServiceType,
			                                                         currentRequest, currentRequestTime);

			service.OnClientRequest(args);
		}

		internal void Response(Message response) {
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