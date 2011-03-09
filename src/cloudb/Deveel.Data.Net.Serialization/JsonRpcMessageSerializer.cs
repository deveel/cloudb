using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Net.Client;

using LitJson;

namespace Deveel.Data.Net.Serialization {
	public sealed class JsonRpcMessageSerializer : TextMessageSerializer, IRpcMessageSerializer {
		public JsonRpcMessageSerializer(Encoding encoding)
			: base(encoding) {
		}

		public JsonRpcMessageSerializer(string encoding)
			: base(encoding) {
		}

		public JsonRpcMessageSerializer() {
		}

		protected override string ContentType {
			get { return "text/json"; }
		}

		private const string DateTimeIso8601Format = "yyyyMMddTHH:mm:s";

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

							if ((string)reader.Value != "2.0")
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

						JsonData data = JsonMapper.ToObject(reader);
						if (!data.IsArray)
							throw new FormatException();

						for (int i = 0; i < data.Count; i++) {
							
						}
					} else {
						if (!reader.Read())
							throw new FormatException();

						if (reader.Token == JsonToken.ArrayStart) {
							List<object> array = new List<object>();
							
						} else {
							message.Attributes.Add(propertyName, JsonMapper.ToObject(reader));
						}
					}
				}
			}

			return message;
		}

		private static void ReadParameters(JsonReader reader, Message message) {
			while (reader.Read()) {
				if (reader.Token == JsonToken.ArrayEnd)
					break;

				message.Arguments.Add(reader.Value);
			}
		}

		private static void WriteValue(object value, string format, JsonWriter writer) {
			// JSON defined types 
			if (value == null)
				writer.Write(null);
			if (value is string)
				writer.Write((string)value);
			if (value is short)
				writer.Write((short)value);
			if (value is int)
				writer.Write((int)value);
			if (value is long)
				writer.Write((long)value);
			if (value is float)
				writer.Write((float)value);
			if (value is double)
				writer.Write((double)value);
			if (value is decimal)
				writer.Write((decimal)value);
			if (value is bool)
				writer.Write((bool)value);

			if (value is DateTime) {
				writer.WriteObjectStart();
				writer.WritePropertyName("$type");
				writer.Write("dateTime");
				if (String.IsNullOrEmpty(format))
					format = DateTimeIso8601Format;
				writer.WritePropertyName("format");
				writer.Write(format);
				writer.Write(((DateTime)value).ToString(format));
				writer.WriteObjectEnd();
			}

			if (value is Array) {
				writer.WriteArrayStart();
				Array array = (Array)value;
				for (int i = 0; i < array.Length; i++) {
					object arrayValue = array.GetValue(i);
					WriteValue(arrayValue, null, writer);
				}
				writer.WriteArrayEnd();
			}
		}

		private static void WriteValue(MessageArgument argument, JsonWriter writer) {
			WriteValue(argument, argument.Format, writer);
		}

		private static void Serialize(Message message, JsonWriter writer, bool inStream) {
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

		public bool SupportsMessageStream {
			get { return true; }
		}
	}
}