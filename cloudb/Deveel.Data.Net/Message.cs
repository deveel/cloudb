using System;
using System.Collections;

namespace Deveel.Data.Net {
	public class Message {
		public Message(string name, object[] args) {
			this.name = name;
			this.args = args == null ? new ArrayList(8) : new ArrayList(args);
		}

		public Message(string name)
			: this(name, null) {
		}

		private readonly string name;
		private readonly ArrayList args;

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