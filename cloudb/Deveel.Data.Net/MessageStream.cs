using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class MessageStream : IEnumerable<Message> {
		public MessageStream(int size) {
			items = new List<object>(size);
		}

		private readonly List<object> items;

		internal const string MessageOpen = "[";
		internal const string MessageClose = "]";
		
		internal IList Items {
			get { return items; }
		}

		public void StartMessage(string messageName) {
			items.Add(messageName);
			items.Add(MessageOpen);
		}

		public void CloseMessage() {
			items.Add(MessageClose);
		}

		public void AddErrorMessage(ServiceException error) {
			StartMessage("E");
			AddMessageArgument(error);
			CloseMessage();
		}

		public void AddMessageArgument(object value) {
			if (value is string)
				value = new StringArgument((string)value);
			
			items.Add(value);
		}

		public void AddMessage(Message message) {
			StartMessage(message.Name);
			for (int i = 0; i < message.ArgumentCount; i++) {
				AddMessageArgument(message[i]);
			}
			CloseMessage();
		}
		
		public void AddMessage(string name, params object[] args) {
			AddMessage(new Message(name, args));
		}

		#region Implementation of IEnumerable

		public IEnumerator<Message> GetEnumerator() {
			return new MessageEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#endregion

		#region MessageEnumerator

		private class MessageEnumerator : IEnumerator<Message> {
			public MessageEnumerator(MessageStream stream) {
				this.stream = stream;
			}

			private readonly MessageStream stream;
			private int index = -1;

			#region Implementation of IDisposable

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				return ++index < stream.items.Count;
			}

			public void Reset() {
				index = -1;
			}

			public Message Current {
				get {
					string msgName = (string)stream.items[index];
					Message message = msgName.Equals("E") ? new ErrorMessage() : new Message(msgName);
					while (++index < stream.items.Count) {
						object v = stream.items[index];
						if (v == null) {
							message.AddArgument(v);
						} else if (v is string) {
							if (v.Equals(MessageOpen))
								continue;
							if (v.Equals(MessageClose))
								return message;
							throw new FormatException("Invalid message format");
						} else if (v is StringArgument) {
							message.AddArgument(((StringArgument)v).Value);
						} else {
							message.AddArgument(v);
						}
					}

					throw new FormatException("No termination found in the message.");
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion
		}

		#endregion

		#region StringArgument

		internal class StringArgument {
			public StringArgument(string value) {
				Value = value;
			}

			public readonly string Value;
		}

		#endregion
	}
}