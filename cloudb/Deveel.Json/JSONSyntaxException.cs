using System;

namespace Deveel.Json {
	internal class JSONSyntaxException : JSONException {
		internal JSONSyntaxException(string message, int index, int line, int character)
			: base(message) {
			this.index = index;
			this.line = line;
			this.character = character;
		}

		private readonly int index;
		private readonly int line;
		private readonly int character;

		public int Character {
			get { return character; }
		}

		public int Line {
			get { return line; }
		}

		public int Index {
			get { return index; }
		}

		public override string Message {
			get { return base.Message + " at " + index + " [" + character + ":" + line + "]"; }
		}
	}
}