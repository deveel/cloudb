using System;

namespace Deveel.Data.Net.Serialization {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class MessageSerializerAttribute : Attribute {
		private readonly Type serializerType;
		private readonly string serializerName;
		private readonly bool withName;

		public MessageSerializerAttribute(Type serializerType) {
			if (serializerType == null) 
				throw new ArgumentNullException("serializerType");

			this.serializerType = serializerType;
			withName = false;
		}

		public MessageSerializerAttribute(string serializerName) {
			if (String.IsNullOrEmpty(serializerName))
				throw new ArgumentNullException("serializerName");

			this.serializerName = serializerName;
			withName = true;
		}

		internal bool WithName {
			get { return withName; }
		}

		public string SerializerName {
			get { return serializerName; }
		}

		public Type SerializerType {
			get { return serializerType; }
		}
	}
}