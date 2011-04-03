using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

using Deveel.Data.Net.Client;

using LitJson;

namespace Deveel.Data.Net.Serialization {
	public sealed class JsonRpcMessageSerializer : TextMessageSerializer, IRpcMessageSerializer {
		private readonly List<IJsonRpcTypeResolver> resolvers = new List<IJsonRpcTypeResolver>();

		public JsonRpcMessageSerializer(Encoding encoding)
			: base(encoding) {
				resolvers.Add(new IRpcTypeResolver(this));
		}

		public JsonRpcMessageSerializer(string encoding)
			: base(encoding) {
				resolvers.Add(new IRpcTypeResolver(this));
		}

		public JsonRpcMessageSerializer() {
			resolvers.Add(new IRpcTypeResolver(this));
		}

		protected override string ContentType {
			get { return "text/json"; }
		}

		public bool SupportsMessageStream {
			get { return true; }
		}

		private const string DateTimeIso8601Format = "yyyyMMddTHH:mm:s";

		public IJsonRpcTypeResolver TypeResolver {
			get { return resolvers.Count == 2 ? resolvers[1] : null; }
			set {
				resolvers.Clear();
				resolvers.Add(new IRpcTypeResolver(this));
				if (value != null)
					resolvers.Add(value);
			}
		}

		public void AddTypeResolver(IJsonRpcTypeResolver resolver) {
			if (resolvers == null)
				throw new ArgumentNullException("resolver");

			for (int i = 0; i < resolvers.Count; i++) {
				if (resolvers[i].GetType() == resolver.GetType())
					throw new ArgumentException("Another resolver of type '" + resolver.GetType() + "' was already present.");
			}

			resolvers.Add(resolver);
		}

		private Type ResolveType(string typeName, out IJsonRpcTypeResolver resolver) {
			for (int i = 0; i < resolvers.Count; i++) {
				Type type;
				resolver = resolvers[i];
				if ((type = resolver.ResolveType(typeName)) != null) {
					return type;
				}
			}

			resolver = null;
			return null;
		}

		private string ResolveTypeName(object value, out IJsonRpcTypeResolver resolver) {
			for (int i = 0; i < resolvers.Count; i++) {
				string elementName;
				resolver = resolvers[i];
				if (!String.IsNullOrEmpty(elementName = resolver.ResolveTypeName(value.GetType()))) {
					return elementName;
				}
			}

			resolver = null;
			return null;
		}

		private string Format(object value, string format) {
			if (value is IFormattable && !String.IsNullOrEmpty(format))
				return ((IFormattable)value).ToString(format, CultureInfo.InvariantCulture);
			return value.ToString();
		}

		private static MessageError ReadMessageError(JsonReader reader) {
			if (!reader.Read())
				throw new FormatException();
			if (reader.Token != JsonToken.ObjectStart)
				throw new FormatException();

			string source = null, message = null, stackTrace = null;
			while (reader.Read()) {
				if (reader.Token == JsonToken.ObjectEnd)
					break;

				if (reader.Token != JsonToken.PropertyName)
					throw new FormatException();

				string propertyName = (string) reader.Value;

				if (!reader.Read())
					throw new FormatException();
				if (reader.Token != JsonToken.String)
					throw new FormatException();

				if (propertyName == "source")
					source = (string)reader.Value;
				else if (propertyName == "message")
					message = (string)reader.Value;
				else if (propertyName == "stackTrace")
					stackTrace = (string)reader.Value;
				else
					throw new FormatException();
			}

			return new MessageError(source, message, stackTrace);
		}

		private void ReadInto(JsonReader reader, JsonWriter jsonWriter, bool firstLevel, out Type type, out IJsonRpcTypeResolver resolver) {
			type = null;
			resolver = null;

			while (reader.Read()) {
				if (reader.Token == JsonToken.PropertyName) {
					string propertyName = (string)reader.Value;

					if (firstLevel && propertyName == "$type") {
						if (!reader.Read())
							throw new FormatException();
						if (reader.Token != JsonToken.String)
							throw new FormatException();

						string typeString = (string)reader.Value;

						type = ResolveType(typeString, out resolver);
						if (type == null)
							throw new FormatException();
					} else {
						jsonWriter.WritePropertyName(propertyName);
					}
				} else if (reader.Token == JsonToken.Boolean) {
					jsonWriter.Write((bool)reader.Value);
				} else if (reader.Token == JsonToken.Int) {
					jsonWriter.Write((int)reader.Value);
				} else if (reader.Token == JsonToken.Long) {
					jsonWriter.Write((long)reader.Value);
				} else if (reader.Token == JsonToken.Double) {
					jsonWriter.Write((double)reader.Value);
				} else if (reader.Token == JsonToken.String) {
					jsonWriter.Write((string)reader.Value);
				} else if (reader.Token == JsonToken.Null) {
					jsonWriter.Write(null);
				} else if (reader.Token == JsonToken.ArrayStart) {
					jsonWriter.WriteArrayStart();

					while (reader.Read()) {
						Type dummyType;
						IJsonRpcTypeResolver dummyResolver;
						ReadInto(reader, jsonWriter, false, out dummyType, out dummyResolver);
						if (reader.Token == JsonToken.ArrayEnd) {
							jsonWriter.WriteArrayEnd();
							break;
						}
					}
				} else if (reader.Token == JsonToken.ObjectEnd) {
					jsonWriter.WriteObjectEnd();
					break;
				}
			}

		}

		private object ReadValue(JsonReader reader) {
			if (reader.Token == JsonToken.Boolean)
				return (bool) reader.Value;
			if (reader.Token == JsonToken.Int)
				return (int) reader.Value;
			if (reader.Token == JsonToken.Long)
				return (long) reader.Value;
			if (reader.Token == JsonToken.Double)
				return (double) reader.Value;
			if (reader.Token == JsonToken.String)
				return (string) reader.Value;
			if (reader.Token == JsonToken.Null)
				return null;

			if (reader.Token == JsonToken.ObjectStart) {
				StringBuilder sb = new StringBuilder();
				JsonWriter jsonWriter = new JsonWriter(sb);
				jsonWriter.WriteObjectStart();

				Type type;
				IJsonRpcTypeResolver resolver;
				ReadInto(reader, jsonWriter, true, out type, out resolver);

				jsonWriter.TextWriter.Flush();

				if (resolver == null)
					throw new FormatException();

				JsonReader jsonReader = new JsonReader(sb.ToString());
				return resolver.ReadValue(jsonReader, type);
			}

			throw new FormatException();
		}

		//TODO:
		private Message Deserialize(JsonReader reader, MessageType messageType) {
			Message message = null;

			while (reader.Read()) {
				JsonToken jsonToken = reader.Token;
				
				if (jsonToken == JsonToken.ObjectStart)
					continue;
				if (jsonToken == JsonToken.ObjectEnd)
					return message;

				if (jsonToken == JsonToken.PropertyName) {
					string propertyName = (string) reader.Value;

					if (String.IsNullOrEmpty(propertyName))
						throw new FormatException();

					if (message == null) {
						if (propertyName == "jsonrpc") {
							if (!reader.Read())
								throw new FormatException();
							if (reader.Token != JsonToken.String)
								throw new FormatException();

							if ((string)reader.Value != "1.0")
								throw new FormatException("JSON RPC protocol version not supported.");

						} else if (propertyName == "method") {
							if (!reader.Read())
								throw new FormatException();
							if (reader.Token != JsonToken.String)
								throw new FormatException("Expected method name.");

							message = new RequestMessage((string) reader.Value);
						} else if (propertyName == "result") {
							if (messageType != MessageType.Response)
								throw new FormatException("Unexpected result message.");

							message = new ResponseMessage();
						} else if (propertyName == "error") {
							if (messageType != MessageType.Response)
								throw new FormatException("Unexpected error result message.");

							message = new ResponseMessage();
							message.Arguments.Add(ReadMessageError(reader));
						} else if (propertyName == "stream") {
							// Addition to support IRPC
							message = new MessageStream(messageType);

							while (reader.Read()) {
								if (reader.Token == JsonToken.ObjectEnd)
									break;

								((MessageStream)message).AddMessage(Deserialize(reader, messageType));
							}

							return message;
						}
					} else if (propertyName == "params") {
						if (!reader.Read())
							throw new FormatException();
						if (reader.Token != JsonToken.ArrayStart)
							throw new FormatException();

						while (reader.Read()) {
							message.Arguments.Add(ReadValue(reader));

							if (reader.Token == JsonToken.ArrayEnd)
								break;
						}
					}
				}
			}

			return message;
		}


		private void WriteValue(object value, string format, JsonWriter writer) {
			// JSON defined types 
			if (value == null || value == DBNull.Value)
				writer.Write(null);
			else if (value is string)
				writer.Write((string)value);
			else if (value is short)
				writer.Write((short)value);
			else if (value is int)
				writer.Write((int)value);
			else if (value is long)
				writer.Write((long)value);
			else if (value is float)
				writer.Write((float)value);
			else if (value is double)
				writer.Write((double)value);
			else if (value is decimal)
				writer.Write((decimal)value);
			else if (value is bool)
				writer.Write((bool)value);
			else if (value is Array) {
				writer.WriteArrayStart();
				Array array = (Array)value;
				for (int i = 0; i < array.Length; i++) {
					object arrayValue = array.GetValue(i);
					WriteValue(arrayValue, null, writer);
				}
				writer.WriteArrayEnd();	
			} else {
				IJsonRpcTypeResolver resolver;
				string typeName = ResolveTypeName(value, out resolver);
				resolver.WriteValue(writer, typeName, value, format);
			}
		}

		private void WriteValue(MessageArgument argument, JsonWriter writer) {
			WriteValue(argument.Value, argument.Format, writer);
		}

		private void Serialize(Message message, JsonWriter writer, bool inStream) {
			if (!inStream) {
				writer.WriteObjectStart();
				writer.WritePropertyName("jsonrpc");
				writer.Write("1.0");
			}

			if (message.Attributes.Contains("id")) {
				writer.WritePropertyName("id");
				// TODO: we presume is a number castable to int4 ...
				writer.Write((int)message.Attributes["id"]);
			}

			if (message is MessageStream) {
				writer.WritePropertyName("stream");
				writer.WriteArrayStart();
				foreach (Message m in ((MessageStream)message)) {
					Serialize(m, writer, true);
				}
				writer.WriteArrayEnd();
			} else if (message.MessageType == MessageType.Request) {
				writer.WritePropertyName("method");
				writer.Write(message.Name);
				writer.WritePropertyName("params");
				writer.WriteArrayStart();
				foreach (MessageArgument argument in message.Arguments) {
					WriteValue(argument, writer);
				}
				writer.WriteArrayEnd();
			} else {
				writer.WritePropertyName("result");
				IList<MessageArgument> args = message.Arguments;
				if (args.Count == 0) {
					writer.Write(null);
				} else if (args.Count == 1) {
					if (message.HasError) {
						writer.Write(null);
						writer.WritePropertyName("error");
						writer.WriteObjectStart();
						writer.WritePropertyName("message");
						writer.Write(message.ErrorMessage);
						writer.WritePropertyName("source");
						writer.Write(message.Error.Source);
						writer.WritePropertyName("stackTrace");
						//TODO: does the string needs to be normalized?
						writer.Write(message.ErrorStackTrace);
						writer.WriteObjectEnd();
					} else {
						WriteValue(args[0], writer);
						writer.WritePropertyName("error");
						writer.Write(null);
					}
				} else {
					// here we are sure we don't have an error, so skip
					// any check about it
					writer.WriteArrayStart();
					foreach (MessageArgument argument in message.Arguments) {
						WriteValue(argument, writer);
					}
					writer.WriteArrayEnd();
				}
			}

			if (!inStream)
				writer.WriteObjectEnd();
		}

		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			JsonReader jsonReader = new JsonReader(reader);
			return Deserialize(jsonReader, messageType);
		}

		protected override void Serialize(Message message, TextWriter writer) {
			JsonWriter jsonWriter = new JsonWriter(writer);
			Serialize(message, jsonWriter, false);
		}

		#region IRpcTypeResolver

		class IRpcTypeResolver : IJsonRpcTypeResolver {
			private readonly JsonRpcMessageSerializer serializer;

			public IRpcTypeResolver(JsonRpcMessageSerializer serializer) {
				this.serializer = serializer;
			}

			public Type ResolveType(string typeName) {
				if (typeName == "dateTime")
					return typeof(DateTime);

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
				if (type == typeof(DateTime))
					return "dateTime";

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

			public void WriteValue(JsonWriter writer, string typeName, object value, string format) {
				writer.WriteObjectStart();
				writer.WritePropertyName("$type");
				writer.Write(typeName);

				if (typeName == "dateTime") {
					if (String.IsNullOrEmpty(format))
						format = DateTimeIso8601Format;
					writer.WritePropertyName("format");
					writer.Write(format);
					writer.WritePropertyName("value");
					writer.Write(((DateTime)value).ToString(format));
				} else if (typeName == "dataAddress") {
					DataAddress dataAddress = (DataAddress)value;
					writer.WritePropertyName("block-id");
					writer.Write(dataAddress.BlockId);
					writer.WritePropertyName("data-id");
					writer.Write(dataAddress.DataId);
				} else if (typeName == "serviceAddress") {
					IServiceAddress serviceAddress = (IServiceAddress)value;
					writer.WritePropertyName("address");
					writer.Write(serviceAddress.ToString());
				} else if (typeName == "singleNodeSet" ||
					typeName == "compressedNodeSet") {
					NodeSet nodeSet = (NodeSet)value;

					writer.WritePropertyName("nids");
					writer.WriteArrayStart();
					for (int i = 0; i < nodeSet.NodeIds.Length; i++) {
						writer.Write(nodeSet.NodeIds[i]);
					}
					writer.WriteArrayEnd();

					writer.WritePropertyName("data");
					string base64Data = Convert.ToBase64String(nodeSet.Buffer);
					writer.Write(base64Data);
				} else {
					throw new FormatException();
				}

				writer.WriteObjectEnd();
			}

			public object ReadValue(JsonReader reader, Type type) {
				if (!reader.Read())
					throw new FormatException();
				if (reader.Token != JsonToken.ObjectStart)
					throw new FormatException();

				if (type == typeof(DateTime)) {
					string propertyName = null, format = null, value = null;
					while (reader.Read()) {
						if (reader.Token == JsonToken.PropertyName) {
							propertyName = (string)reader.Value;
						} else if (reader.Token == JsonToken.String) {
							if (propertyName == null)
								throw new FormatException();
							if (propertyName == "format") {
								format = (string)reader.Value;
							} else if (propertyName == "value") {
								value = (string)reader.Value;
							} else {
								throw new FormatException();
							}
						} else if (reader.Token == JsonToken.ObjectEnd) {
							break;
						}
					}

					if (String.IsNullOrEmpty(value))
						throw new FormatException();

					return !String.IsNullOrEmpty(format)
							? DateTime.ParseExact(value, format, CultureInfo.InvariantCulture)
							: DateTime.Parse(value, CultureInfo.InvariantCulture);
				}
				if (type == typeof(DataAddress)) {
					long blockId = -1;
					int dataId = -1;
					string propertyName = null;
					while (reader.Read()) {
						if (reader.Token == JsonToken.PropertyName) {
							propertyName = (string)reader.Value;
						} else if (reader.Token == JsonToken.Long) {
							if (propertyName == null)
								throw new FormatException();

							if (propertyName == "block-id")
								throw new FormatException();

							blockId = (long)reader.Value;
						} else if (reader.Token == JsonToken.Int) {
							if (propertyName == null)
								throw new FormatException();

							if (propertyName != "data-id")
								throw new FormatException();

							dataId = (int)reader.Value;
						} else if (reader.Token == JsonToken.ObjectEnd) {
							break;
						}
					}

					return new DataAddress(blockId, dataId);
				}

				if (typeof(IServiceAddress).IsAssignableFrom(type)) {
					string propertyName = null;
					string value = null;
					int code = -1;
					while (reader.Read()) {
						if (reader.Token == JsonToken.PropertyName) {
							propertyName = (string)reader.Value;
						} else if (reader.Token == JsonToken.String) {
							if (propertyName == null)
								throw new FormatException();
							if (propertyName != "value")
								throw new FormatException();

							value = (string)reader.Value;
						} else if (reader.Token == JsonToken.Int) {
							if (propertyName == null)
								throw new FormatException();
							if (propertyName != "code")
								throw new FormatException();

							code = (int)reader.Value;
						} else if (reader.Token == JsonToken.ObjectEnd) {
							break;
						}
					}

					if (code == -1)
						throw new FormatException();
					if (value == null)
						throw new FormatException();

					Type addressType = ServiceAddresses.GetAddressType(code);
					if (addressType == null)
						throw new FormatException();

					IServiceAddressHandler handler = ServiceAddresses.GetHandler(addressType);
					return handler.FromString(value);
				} else if (typeof(NodeSet).IsAssignableFrom(type)) {
					NodeSet nodeSet = null;
					List<long> nodeIds = new List<long>();
					byte[] buffer = null;

					string propertyName = null;

					while (reader.Read()) {
						if (reader.Token == JsonToken.PropertyName) {
							propertyName = (string) reader.Value;
						} else if (reader.Token == JsonToken.String) {
							if (propertyName == null)
								throw new FormatException();
							if (propertyName != "data")
								throw new FormatException();

							buffer = Convert.FromBase64String((string) reader.Value);
						} else if (reader.Token == JsonToken.ArrayStart) {
							while (reader.Read()) {
								if (reader.Token == JsonToken.ArrayEnd)
									break;
							}

							nodeIds.Add((long)reader.Value);
						}
					}

					if (type == typeof(SingleNodeSet)) {
						nodeSet = new SingleNodeSet(nodeIds.ToArray(), buffer);
					} else if (type == typeof(CompressedNodeSet)) {
						nodeSet = new CompressedNodeSet(nodeIds.ToArray(), buffer);
					} else {
						throw new FormatException();
					}

					return nodeSet;
				}
					
				throw new FormatException();
			}
		}

		#endregion
	}
}