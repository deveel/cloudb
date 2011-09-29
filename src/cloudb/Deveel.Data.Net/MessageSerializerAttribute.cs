using System;

namespace Deveel.Data.Net {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class MessageSerializerAttribute : Attribute {
		private readonly Type serializerType;

		public MessageSerializerAttribute(Type serializerType) {
			if (serializerType == null) 
				throw new ArgumentNullException("serializerType");

			this.serializerType = serializerType;
		}

		public Type SerializerType {
			get { return serializerType; }
		}
	}
}