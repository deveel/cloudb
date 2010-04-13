using System;

namespace Deveel.Data.Net {
	public sealed class Service {
		internal Service(ServiceAddress address) {
			this.address = address;
		}

		private readonly ServiceAddress address;
		private ServiceType type;

		private long memoryUsed;
		private long memoryTotal;
		private long storageUsed;
		private long storageTotal;

		public ServiceType Type {
			get { return type; }
			internal set { type = value; }
		}

		public ServiceAddress Address {
			get { return address; }
		}

		public long MemoryUsed {
			get { return memoryUsed; }
			internal set { memoryUsed = value; }
		}

		public long MemoryTotal {
			get { return memoryTotal; }
			internal set { memoryTotal = value; }
		}

		public long StorageUsed {
			get { return storageUsed; }
			internal set { storageUsed = value; }
		}

		public long StorageTotal {
			get { return storageTotal; }
			internal set { storageTotal = value; }
		}
	}
}