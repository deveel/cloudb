using System;

namespace Deveel.Data.Net.Client {
	public abstract class Message : IAttributesHandler {
		private string name;
		internal MessageArguments arguments;
		internal MessageAttributes attributes;
		private bool readOnly;

		internal Message(string name) {
			this.name = name;
			arguments = new MessageArguments(false);
			attributes = new MessageAttributes(this);
		}

		public bool HasName {
			get { return !String.IsNullOrEmpty(name); }
		}

		public virtual string Name {
			get { return name; }
			set {
				if (readOnly)
					throw new InvalidOperationException();

				name = value;
			}
		}

		public abstract MessageType MessageType { get; }

		public virtual MessageAttributes Attributes {
			get { return attributes; }
		}

		public virtual MessageArguments Arguments {
			get { return arguments; }
		}

		bool IAttributesHandler.IsReadOnly {
			get { return readOnly; }
		}

		public bool HasError {
			get { return GetError(this) != null; }
		}

		public MessageError Error {
			get { return GetError(this); }
		}

		public string ErrorMessage {
			get { return GetErrorMessage(this); }
		}

		public string ErrorStackTrace {
			get { return GetErrorStackTrace(this); }
		}

		internal void Seal() {
			readOnly = true;
			arguments.Seal();
		}

		public static MessageError GetError(Message message) {
			if (message is MessageStream) {
				foreach (Message msg in (MessageStream)message) {
					MessageError error = GetError(msg);
					if (error != null)
						return error;
				}

				return null;
			}

			return message.Arguments.Count == 1 && message.Arguments[0].Value is MessageError
			       	? (MessageError) message.Arguments[0].Value
			       	: null;
		}

		public static string GetErrorMessage(Message message) {
			MessageError error = GetError(message);
			return error == null ? null : error.Message;
		}

		public static string GetErrorStackTrace(Message message) {
			MessageError error = GetError(message);
			return error == null ? null : error.StackTrace;
		}
	}
}