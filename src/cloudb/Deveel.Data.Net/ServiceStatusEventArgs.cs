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
	public delegate void ServiceStatusEventHandler(object sender, ServiceStatusEventArgs args);

	public sealed class ServiceStatusEventArgs : EventArgs {
		private readonly IServiceAddress serviceAddress;
		private readonly ServiceType serviceType;
		private readonly ServiceStatus oldStatus;
		private readonly ServiceStatus newStatus;

		internal ServiceStatusEventArgs(IServiceAddress serviceAddress, ServiceType serviceType, ServiceStatus oldStatus, ServiceStatus newStatus) {
			this.serviceAddress = serviceAddress;
			this.serviceType = serviceType;
			this.oldStatus = oldStatus;
			this.newStatus = newStatus;
		}

		public ServiceStatus NewStatus {
			get { return newStatus; }
		}

		public ServiceStatus OldStatus {
			get { return oldStatus; }
		}

		public ServiceType ServiceType {
			get { return serviceType; }
		}

		public IServiceAddress ServiceAddress {
			get { return serviceAddress; }
		}
	}
}