using System;

namespace Deveel.Data {
	public sealed class ErrorStateException : SystemException {
		public ErrorStateException(string message, Exception e)
			: base(message, e) {
		}
		
		public ErrorStateException(string message)
			: base(message) {
		}
		
		public ErrorStateException(Exception e)
			: base(e.Message, e) {
		}
	}
}