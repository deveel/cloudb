using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Net.Serialization {
	public static class MessageSerializers {
		private static readonly Dictionary<string, IMessageSerializer> SerializersByName;
		private static readonly Dictionary<string, Type> SerializerTypes;
		private static readonly Dictionary<Type, IMessageSerializer> SerializersByType;

		static MessageSerializers() {
			SerializersByName = new Dictionary<string, IMessageSerializer>(StringComparer.InvariantCultureIgnoreCase);
			SerializersByType = new Dictionary<Type, IMessageSerializer>();
			SerializerTypes = new Dictionary<string, Type>();

			RetrieveSerializers();
		}

		private static void RetrieveSerializers() {
			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			for (int i = 0; i < assemblies.Length; i++) {
				Assembly assembly = assemblies[i];
				Type[] types = assembly.GetTypes();
				for (int j = 0; j < types.Length; j++) {
					Type type = types[j];
					if (typeof(IMessageSerializer).IsAssignableFrom(type) &&
						type != typeof(IMessageSerializer) && !type.IsAbstract) {
						SerializerNameAttribute nameAttribute =
							(SerializerNameAttribute) Attribute.GetCustomAttribute(type, typeof (SerializerNameAttribute));
						if (nameAttribute != null &&
							!SerializerTypes.ContainsKey(nameAttribute.Name)) {
							SerializerTypes[nameAttribute.Name] = type;
						} else {
							string name = type.Name;
							int index = name.IndexOf("Serializer");
							if (index > 0)
								name = name.Substring(0, index);
							SerializerTypes[name] = type;
						}
					}
				}
			}
		}

		public static IMessageSerializer GetSerializer(string name) {
			return GetSerializer(name, new object[0]);
		}

		public static IMessageSerializer GetSerializer(string name, params object[] args) {
			if (String.IsNullOrEmpty(name))
				return GetDefaultSerializer();

			IMessageSerializer serializer;
			if (!SerializersByName.TryGetValue(name, out serializer)) {
				Type type;
				if (!SerializerTypes.TryGetValue(name, out type))
					return null;

				if (SerializersByType.TryGetValue(type, out serializer))
					return serializer;

				serializer = Activator.CreateInstance(type, args) as IMessageSerializer;

				SerializersByName[name] = serializer;
				SerializersByType[type] = serializer;
			}

			return serializer;
		}

		private static IMessageSerializer GetDefaultSerializer() {
			IEnumerator<KeyValuePair<string, IMessageSerializer>> en = SerializersByName.GetEnumerator();
			if (!en.MoveNext())
				return null;
			return en.Current.Value;
		}

		public static IMessageSerializer GetSerializer(Type serializerType) {
			return GetSerializer(serializerType, null);
		}

		public static IMessageSerializer GetSerializer(Type serializerType, params object[] args) {
			if (serializerType == null)
				throw new ArgumentNullException("serializerType");
			if (!typeof(IMessageSerializer).IsAssignableFrom(serializerType))
				throw new ArgumentException("The type '" + serializerType + "' is not a message serializer.");

			IMessageSerializer serializer;
			if (!SerializersByType.TryGetValue(serializerType, out serializer)) {
				serializer = Activator.CreateInstance(serializerType, args) as IMessageSerializer;
				SerializersByType[serializerType] = serializer;
			}

			return serializer;
		}
	}
}