using System;

namespace Deveel.Data.Net {
	public class BlockServerElement {
		public BlockServerElement(IServiceAddress address, ServiceStatus status) {
			this.address = address;
			this.status = status;
		}

		private readonly IServiceAddress address;
		private readonly ServiceStatus status;

		public IServiceAddress Address {
			get { return address; }
		}

		public bool IsStatusUp {
			get { return Status == ServiceStatus.Up; }
		}

		public ServiceStatus Status {
			get { return status; }
		}
	}
}