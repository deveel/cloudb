using System;

using Newtonsoft.Json;

namespace Deveel.Data.Net.Client {
	public interface IJsonRpcTypeResolver {
		Type ResolveType(string typeName);

		string ResolveTypeName(Type type);

		void WriteValue(JsonWriter xmlWriter, string typeName, object value, string format);

		object ReadValue(JsonReader reader, Type type);
	}
}