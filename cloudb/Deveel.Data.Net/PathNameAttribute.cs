using System;

namespace Deveel.Data.Net {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple=false)]
	public sealed class PathNameAttribute : Attribute {
		private string name;
		private string description;
		
		public PathNameAttribute(string name, string description) {
			this.name = name;
			this.description = description;
		}
		
		public PathNameAttribute(string name)
			: this(name, null) {
		}
		
		public PathNameAttribute()
			: this(null) {
		}
		
		public string Description {
			get { return description; }
			set { description = value; }
		}
		
		public string Name {
			get { return name; }
			set { name = value; }
		}
	}
}