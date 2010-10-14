using System;

using Deveel.Console;

namespace Deveel.Data.Net {
	internal class NetworkContext : IExecutionContext {
		private readonly NetworkProfile netProfile;

		public NetworkContext(NetworkProfile netProfile) {
			this.netProfile = netProfile;
		}

		public void Dispose() {
		}

		public PropertyRegistry Properties {
			get { return null; }
		}

		public bool IsIsolated {
			get { return true; }
		}

		public NetworkProfile Network {
			get { return netProfile; }
		}
	}
}