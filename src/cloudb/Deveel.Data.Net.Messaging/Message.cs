using System;

namespace Deveel.Data.Net.Messaging {
	public class Message {
		private string name;
		private readonly MessageArguments arguments;
		private bool readOnly;

		public Message() 
			: this((string)null) {
		}

		public Message(string name)
			: this(name, null) {
		}

		public Message(params object[] args) 
			: this(null, args) {
		}

		public Message(string name, params object[] args) {
			this.name = name;
			arguments = new MessageArguments(false);			
			if (args != null) {
				foreach (object arg in args) {
					arguments.Add(arg);
				}
			}
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

		public virtual MessageArguments Arguments {
			get { return arguments; }
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

		private static string GetErrorMessage(Message message) {
			MessageError error = GetError(message);
			return (error == null ? null : error.Message);
		}

		public string ErrorStackTrace {
			get { return GetErrorStackTrace(this); }
		}

		private static string GetErrorStackTrace(Message message) {
			MessageError error = GetError(message);
			return (error == null ? null : error.StackTrace);
		}

		private static MessageError GetError(Message message) {
			foreach (var argument in message.Arguments) {
				if (argument.Value is MessageError)
					return (argument.Value as MessageError);
			}

			return null;
		}

		internal void Seal() {
			readOnly = true;
			arguments.Seal();
		}

		public MessageStream AsStream() {
			MessageStream stream = new MessageStream();
			stream.AddMessage(this);
			return stream;
		}
	}
}