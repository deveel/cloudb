using System;
using System.Collections.Generic;
using System.Text;

using Newtonsoft.Json;

namespace Deveel.Data.Net.Client {
	public sealed class JsonRpcMessageSerializer : JsonMessageSerializer, IMessageStreamSupport {
		private readonly List<IJsonRpcTypeResolver> resolvers = new List<IJsonRpcTypeResolver>();

		public JsonRpcMessageSerializer(string encoding)
			: base(encoding) {
		}

		public JsonRpcMessageSerializer(Encoding encoding)
			: base(encoding) {
		}

		public JsonRpcMessageSerializer() {
		}

		public IJsonRpcTypeResolver TypeResolver {
			get { return resolvers.Count == 2 ? resolvers[1] : null; }
			set {
				resolvers.Clear();
				resolvers.Add(new IRpcTypeResolver(this));
				if (value != null)
					resolvers.Add(value);
			}
		}

		protected override void Serialize(Message message, JsonTextWriter writer) {
			throw new NotImplementedException();
		}

		protected override Message Deserialize(JsonTextReader reader, MessageType messageType) {
			throw new NotImplementedException();
		}

		#region IRpcTypeResolver

		private class IRpcTypeResolver : IJsonRpcTypeResolver {
			private readonly JsonRpcMessageSerializer serializer;

			public IRpcTypeResolver(JsonRpcMessageSerializer serializer) {
				this.serializer = serializer;
			}

			public Type ResolveType(string typeName) {
				if (typeName == "dataAddress")
					return typeof(DataAddress);
				if (typeName == "singleNodeSet")
					return typeof(SingleNodeSet);
				if (typeName == "compressedNodeSet")
					return typeof(CompressedNodeSet);
				if (typeName == "serviceAddress")
					return typeof(IServiceAddress);

				return null;
			}

			public string ResolveTypeName(Type type) {
				if (type == typeof(DataAddress))
					return "dataAddress";
				if (type == typeof(SingleNodeSet))
					return "singleNodeSet";
				if (type == typeof(CompressedNodeSet))
					return "compressedNodeSet";
				if (typeof(IServiceAddress).IsAssignableFrom(type))
					return "serviceAddress";

				return null;
			}

			public void WriteValue(JsonWriter xmlWriter, string typeName, object value, string format) {
				throw new NotImplementedException();
			}

			public object ReadValue(JsonReader reader, Type type) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}