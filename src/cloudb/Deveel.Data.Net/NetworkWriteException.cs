using System;

namespace Deveel.Data.Net {
	public sealed class NetworkWriteException : ApplicationException {
		internal NetworkWriteException(string message, Exception innerException)
			: base(message, innerException) {
		}

		internal NetworkWriteException(string message)
			: base(message) {
		}
	}
}