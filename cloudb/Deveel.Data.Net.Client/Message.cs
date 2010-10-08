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

		internal void Seal() {
			readOnly = true;
			arguments.Seal();
		}
	}
}