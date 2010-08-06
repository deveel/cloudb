using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public class Message {
		public Message(string name, object[] args) {
			this.name = name;
			this.args = args == null ? new List<object>(8) : new List<object>(args);
		}

		public Message(string name)
			: this(name, null) {
		}

		private readonly string name;
		private readonly List<object> args;

		public string Name {
			get { return name; }
		}
		
				public bool IsError {
			get { return name.Equals("E"); }
		}

		public string ErrorMessage {
			get { return !IsError ? null : Error.Message; }
		}

		public string ErrorStackTrace {
			get { return !IsError ? null : Error.StackTrace; }
		}

		public string ErrorSource {
			get { return !IsError ? null : Error.Source; }
		}

		public ServiceException Error {
			get { return !IsError ? null : (ServiceException) args[0]; }
		}

		public int ArgumentCount {
			get { return args.Count; }
		}

		public object this[int index] {
			get { return args[index]; }
		}

		public Message AddArgument(object value) {
			args.Add(value);
			return this;
		}
	}
}