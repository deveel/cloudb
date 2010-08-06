using System;

namespace Deveel.Data.Net {
	public sealed class MachineProfile {
		internal MachineProfile(ServiceAddress address) {
			this.address = address;
		}

		private readonly ServiceAddress address;
		private ServiceType type;

		private long memoryUsed;
		private long memoryTotal;
		private long storageUsed;
		private long storageTotal;

		private string errorState;
		private bool hasError;

		public ServiceType ServiceType {
			get { return type; }
			internal set { type = value; }
		}

		public bool IsBlock {
			get { return (type & ServiceType.Block) != 0; }
		}

		public bool IsManager {
			get { return (type & ServiceType.Manager) != 0; }
		}

		public bool IsRoot {
			get { return (type & ServiceType.Root) != 0; }
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

		public string ErrorState {
			get { return errorState; }
			internal set {
				errorState = value;
				hasError = (value != null);
			}
		}

		public bool HasError {
			get { return hasError; }
		}
	}
}