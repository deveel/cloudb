using System;

namespace Deveel.Data.Net.Messaging {
	public sealed class MessageError {
		private readonly string message;
		private string stackTrace;
		private readonly string source;
		private readonly string errorType;

		private MessageError(string source, string message, string stackTrace) 
			: this(source, message, stackTrace, null) {
		}

		public MessageError(string source, string message, string stackTrace, string errorType) {
			this.source = source;
			this.stackTrace = stackTrace;
			this.message = message;
			this.errorType = errorType;
		}

		public MessageError(string source, string message)
			: this(source, message, null) {
		}

		public MessageError(string message)
			: this(null, message) {
		}

		public MessageError(Exception exception)
			: this(exception.Source, exception.Message, exception.StackTrace, exception.GetType().Name) {
		}

		public string ErrorType {
			get { return errorType; }
		}

		public string StackTrace {
			get { return stackTrace; }
			set { stackTrace = value; }
		}

		public string Source {
			get { return source; }
		}

		public string Message {
			get { return message; }
		}
		
		public Exception AsException() {
			return new MessageErrorException(this);
		}
		
		#region MessageErrorException
		
		class MessageErrorException : Exception {
			private readonly MessageError error;
			
			public MessageErrorException(MessageError error) {
				this.error = error;
			}
			
			public override string Message {
				get { return error.Message; }
			}
			
			public override string StackTrace {
				get { return error.StackTrace; }
			}
		}
		
		#endregion
	}
}