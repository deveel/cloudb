//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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