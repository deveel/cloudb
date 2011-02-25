using System;

namespace Deveel.Data.Net {
	public class ServiceNotConnectedException : ApplicationException {
		public ServiceNotConnectedException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public ServiceNotConnectedException(string message)
			: base(message) {
		}

		public ServiceNotConnectedException() {
		}
	}
}