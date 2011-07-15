using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public delegate void ClientCommandEventHandler(object sender, ClientCommandEventArgs args);

	public sealed class ClientCommandEventArgs : EventArgs {
		private readonly bool hasResponse;
		private readonly string localEndPoint;
		private readonly string remoteEndPoint;
		private readonly ServiceType serviceType;
		private readonly DateTime requestTime;
		private readonly DateTime responseTime;
		private readonly Message requestMessage;
		private readonly Message responseMessage;

		internal ClientCommandEventArgs(string localEndPoint, string remoteEndPoint, ServiceType serviceType, Message requestMessage, DateTime requestTime) {
			this.localEndPoint = localEndPoint;
			this.remoteEndPoint = remoteEndPoint;
			this.serviceType = serviceType;
			this.requestTime = requestTime;
			this.requestMessage = requestMessage;
		}

		internal ClientCommandEventArgs(string localEndPoint, string remoteEndPoint, ServiceType serviceType, Message requestMessage, DateTime requestTime, Message responseMessage, DateTime responseTime)
			: this(localEndPoint, remoteEndPoint, serviceType, requestMessage, requestTime) {
			this.responseMessage = responseMessage;
			this.responseTime = responseTime;
			hasResponse = true;
		}

		public string RemoteEndPoint {
			get { return remoteEndPoint; }
		}

		public string LocalEndPoint {
			get { return localEndPoint; }
		}

		public Message ResponseMessage {
			get { return responseMessage; }
		}

		public Message RequestMessage {
			get { return requestMessage; }
		}

		public DateTime ResponseTime {
			get { return responseTime; }
		}

		public DateTime RequestTime {
			get { return requestTime; }
		}

		public ServiceType ServiceType {
			get { return serviceType; }
		}

		public bool HasResponse {
			get { return hasResponse; }
		}

		public TimeSpan Elapsed {
			get { return !hasResponse ? TimeSpan.Zero : responseTime - requestTime; }
		}

		public bool HasError {
			get { return hasResponse ? responseMessage.HasError : false; }
		}
	}
}