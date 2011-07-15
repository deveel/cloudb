using System;
using System.Text;

namespace Deveel.Data.Net.Client {
	[Serializable]
	public sealed class PathAttribute {
		private readonly string name;
		private readonly PathValue value;

		public PathAttribute(string name, PathValue value) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			if (value == null)
				value = new PathValue(null);

			this.name = name;
			this.value = value;
		}

		public PathValue Value {
			get { return value; }
		}

		public string Name {
			get { return name; }
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder(name);
			sb.Append(":");
			sb.Append(value.ToString());
			return sb.ToString();
		}
	}
}