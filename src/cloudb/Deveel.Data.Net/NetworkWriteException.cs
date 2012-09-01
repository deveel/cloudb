using System;

namespace Deveel.Data.Net {
	public class NetworkWriteException : ApplicationException {
		public NetworkWriteException(string message)
			: this(message, null) {
		}

		public NetworkWriteException(string message, Exception innerException)
			: base(message, innerException) {
		}
	}
}