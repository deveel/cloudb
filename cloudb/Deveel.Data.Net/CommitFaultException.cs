using System;

namespace Deveel.Data.Net {
	public sealed class CommitFaultException : Exception {
		public CommitFaultException(string message)
			: base(message) {
		}
	}
}