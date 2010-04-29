using System;

namespace Deveel.Data.Net {
	public class BlockServerElement {
		public BlockServerElement(ServiceAddress address, string status) {
			this.address = address;
			this.status = status;
		}

		private readonly ServiceAddress address;
		private readonly String status;

		public ServiceAddress Address {
			get { return address; }
		}

		public bool IsStatusUp {
			get { return Status.StartsWith("U"); }
		}

		public string Status {
			get { return status; }
		}
	}
}