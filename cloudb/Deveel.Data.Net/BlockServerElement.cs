using System;

namespace Deveel.Data.Net {
	public class BlockServerElement {
		public BlockServerElement(IServiceAddress address, string status) {
			this.address = address;
			this.status = status;
		}

		private readonly IServiceAddress address;
		private readonly String status;

		public IServiceAddress Address {
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