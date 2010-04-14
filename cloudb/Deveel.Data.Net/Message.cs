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