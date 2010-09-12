using System;

namespace Deveel.Data.Diagnostics {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class LoggerNameAttribute : Attribute {
		private readonly string name;

		public LoggerNameAttribute(string name) {
			this.name = name;
		}

		public string Name {
			get { return name; }
		}
	}
}