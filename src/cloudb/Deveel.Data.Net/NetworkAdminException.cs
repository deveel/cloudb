using System;

namespace Deveel.Data.Net {
	public class NetworkAdminException : Exception {
		public NetworkAdminException(string message)
			: base(message) {
		}
	}
}