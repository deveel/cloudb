using System;

namespace Deveel.Data.Net {
	public sealed class NetworkException : ApplicationException {
		internal NetworkException(string message)
			: base(message) {
		}
	}
}