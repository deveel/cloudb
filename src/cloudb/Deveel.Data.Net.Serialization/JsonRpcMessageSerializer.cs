using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
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

				string propertyName = null;

				if (jsonToken == JsonToken.PropertyName) {
					propertyName = (string) reader.Value;

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

		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			JsonReader jsonReader = new JsonReader(reader);
			return Deserialize(jsonReader, messageType);
		}

		protected override void Serialize(Message message, TextWriter writer) {
			throw new NotImplementedException();
		}

		public bool SupportsMessageStream {
			get { return true; }
		}
	}
}