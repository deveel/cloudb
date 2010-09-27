using System;

namespace Deveel.Data.Net {
	public sealed class ServiceException : Exception {
		internal ServiceException(string source, string message, string stackTrace) {
			this.source = source;
			this.message = message;
			this.stackTrace = stackTrace;
		}

		internal ServiceException(Exception error)
			: this(error.Source, error.Message, error.StackTrace) {
		}

		private string source;
		private readonly string message;
		private readonly string stackTrace;

		public override string Message {
			get { return message == null ? String.Empty : message; }
		}

		public override string StackTrace {
			get { return stackTrace == null ? String.Empty : stackTrace; }
		}

		public override string Source {
			get { return source == null ? String.Empty : source; }
			set { source = value; }
		}
	}
}