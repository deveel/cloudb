using System;

namespace Deveel.Data.Net.Security {
	public abstract class AuthMessage {
		private readonly object context;
		private readonly string mechanism;
		private bool readOnly;
		private readonly AuthMessageArguments arguments;

		protected AuthMessage(object context, string mechanism) {
			if (context == null) 
				throw new ArgumentNullException("context");
			if (mechanism == null) 
				throw new ArgumentNullException("mechanism");

			this.context = context;
			this.mechanism = mechanism;

			arguments = new AuthMessageArguments(this);
		}

		public object Context {
			get { return context; }
		}

		public AuthMessageArguments Arguments {
			get { return arguments; }
		}

		public string Mechanism {
			get { return mechanism; }
		}

		internal bool IsReadOnly {
			get { return readOnly; }
		}

		internal void Seal() {
			readOnly = true;
		}
	}
}