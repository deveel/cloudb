using System;

namespace Deveel.Data.Net.Serialization {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class SerializerNameAttribute : Attribute {
		private readonly string name;

		public SerializerNameAttribute(string name) {
			this.name = name;
		}

		public string Name {
			get { return name; }
		}
	}
}