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
			this.newStatus = newStatus;
			this.oldStatus = oldStatus;
			this.serviceType = serviceType;
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