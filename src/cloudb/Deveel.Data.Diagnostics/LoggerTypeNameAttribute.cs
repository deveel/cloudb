using System;

namespace Deveel.Data.Diagnostics {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class LoggerTypeNameAttribute : Attribute {
		private readonly string name;

		public LoggerTypeNameAttribute(string name) {
			this.name = name;
		}

		public string Name {
			get { return name; }
		}
	}
}