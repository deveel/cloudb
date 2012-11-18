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