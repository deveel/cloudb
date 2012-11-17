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
	public abstract class ServiceMessageQueue {
		protected readonly List<IServiceAddress> ServiceAddresses;
		protected readonly List<MessageStream> Messages;
		protected readonly List<ServiceType> Types;


		protected ServiceMessageQueue() {

			ServiceAddresses = new List<IServiceAddress>(4);
			Messages = new List<MessageStream>(4);
			Types = new List<ServiceType>(4);

		}

		public void AddMessageStream(IServiceAddress service_address, MessageStream message_stream, ServiceType message_type) {
			ServiceAddresses.Add(service_address);
			Messages.Add(message_stream);
			Types.Add(message_type);
		}

		public abstract void Enqueue();
	}
}