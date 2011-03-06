using System;
using System.IO;

using Deveel.Data.Net.Serialization;

using Newtonsoft.Json;

namespace Deveel.Data.Net.Client {
	public sealed class JsonResttMessageSerializer : TextMessageSerializer {
		protected override string ContentType {
			get { return "application/json"; }
		}

		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			return Deserialize(new JsonTextReader(reader), messageType);
		}

		protected override void Serialize(Message message, TextWriter writer) {
			Serialize(message, new JsonTextWriter(writer));
		}

		public Message Deserialize(JsonReader reader, MessageType messageType) {
			throw new NotImplementedException();
		}

		public void Serialize(Message message, JsonWriter writer) {
			throw new NotImplementedException();
		}
	}
}