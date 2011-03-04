using System;

namespace Deveel.Data.Net {
	public sealed class NetworkAdminException : Exception {
		private readonly string stackTrace;

		internal NetworkAdminException(string message, string stackTrace)
			: base(message) {
			this.stackTrace = stackTrace;
		}

		internal NetworkAdminException(string message)
			: this(message, null) {
		}

		public override string StackTrace {
			get { return stackTrace ?? base.StackTrace; }
		}
	}
}