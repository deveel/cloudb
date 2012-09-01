using System;
using System.Collections.Generic;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public delegate void ClientCommandEventHandler(object sender, ClientCommandEventArgs args);

	public sealed class ClientCommandEventArgs : EventArgs {
		private readonly bool hasResponse;
		private readonly string localEndPoint;
		private readonly string remoteEndPoint;
		private readonly ServiceType serviceType;
		private readonly DateTime requestTime;
		private readonly DateTime responseTime;
		private readonly IEnumerable<Message> requestMessage;
		private readonly IEnumerable<Message> responseMessage;

		internal ClientCommandEventArgs(string localEndPoint, string remoteEndPoint, ServiceType serviceType, IEnumerable<Message> requestMessage, DateTime requestTime) {
			this.localEndPoint = localEndPoint;
			this.remoteEndPoint = remoteEndPoint;
			this.serviceType = serviceType;
			this.requestTime = requestTime;
			this.requestMessage = requestMessage;
		}

		internal ClientCommandEventArgs(string localEndPoint, string remoteEndPoint, ServiceType serviceType, IEnumerable<Message> requestMessage, DateTime requestTime, 
			IEnumerable<Message> responseMessage, DateTime responseTime)
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

		public IEnumerable<Message> ResponseMessage {
			get { return responseMessage; }
		}

		public IEnumerable<Message> RequestMessage {
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

		/*
		public bool HasError {
			get { return hasResponse ? responseMessage.HasError : false; }
		}
		*/
	}
}