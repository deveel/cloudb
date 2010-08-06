using System;

namespace Deveel.Data.Net {
	public sealed class NetworkAdminException : Exception {
		internal NetworkAdminException(string message)
			: base(message) {
		}
	}
}