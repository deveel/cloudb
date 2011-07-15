using System;

using LitJson;

namespace Deveel.Data.Net.Serialization {
	public interface IJsonRpcTypeResolver {
		Type ResolveType(string typeName);

		string ResolveTypeName(Type type);

		void WriteValue(JsonWriter writer, string typeName, object value, string format);

		object ReadValue(JsonReader reader, Type type);
	}
}