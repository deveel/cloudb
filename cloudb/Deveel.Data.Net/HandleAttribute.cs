using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
	public sealed class HandleAttribute : Attribute {
		private readonly string pathTypeName;

		public HandleAttribute(string pathTypeName) {
			this.pathTypeName = pathTypeName;
		}

		public HandleAttribute(Type pathType)
			: this(pathType.FullName + ", " + pathType.Assembly.GetName().Name) {
		}

		public string PathTypeName {
			get { return pathTypeName; }
		}
	}
}