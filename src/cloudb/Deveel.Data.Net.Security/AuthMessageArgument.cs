using System;

namespace Deveel.Data.Net.Security {
	public sealed class AuthMessageArgument {
		private readonly string name;
		private readonly AuthObject value;

		public AuthMessageArgument(string name, AuthObject value) {
			this.name = name;
			this.value = value;
		}

		public AuthMessageArgument(string name, object value)
			: this(name, new AuthObject(value)) {
		}

		public AuthObject Value {
			get { return value; }
		}

		public string Name {
			get { return name; }
		}
	}
}