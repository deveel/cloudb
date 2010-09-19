using System;
using System.IO;
using System.Text;

using Deveel.Json;

namespace Deveel.Data.Net {
	public sealed class JsonMessageStreamSerializer : ITextMessageSerializer {
		private Encoding encoding;

		public JsonMessageStreamSerializer(string  encoding)
			: this(Encoding.GetEncoding(encoding)) {
		}

		public JsonMessageStreamSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		public JsonMessageStreamSerializer()
			: this(Encoding.UTF8) {
		}

		public Encoding Encoding {
			get {
				if (encoding == null)
					encoding = Encoding.UTF8;
				return encoding;
			}
			set { encoding = value; }
		}

		public MessageStream Deserialize(Stream input) {
			StreamReader reader = new StreamReader(new BufferedStream(input), encoding);
			return Deserialize(reader);
		}

		public MessageStream Deserialize(TextReader reader) {
			JSONObject obj = new JSONObject(new JSONReader(reader));
			JSONArray messageArray = obj.GetValue<JSONArray>("message");

			int sz = messageArray.Length;
			if (sz == 0)
				return new MessageStream(0);

			MessageStream messageStream = new MessageStream(16);

			for (int i = 0; i < sz; i++) {
				JSONObject message = (JSONObject) messageArray[i];
				string name = message.GetValue<string>("name");
				JSONArray messageArgs = message.GetValue<JSONArray>("args");

				messageStream.StartMessage(name);

				for (int j = 0; j < messageArgs.Length; j++) {
					object arg = messageArgs[j];

					if (arg is JSONObject) {
						
					} else {
						messageStream.AddMessageArgument(arg);
					}
				}

				messageStream.CloseMessage();
			}

			return messageStream;
		}

		public void Serialize(MessageStream messageStream, Stream output) {
			throw new NotImplementedException();
		}

		string ITextMessageSerializer.ContentEncoding {
			get { return Encoding.EncodingName; }
		}

		string ITextMessageSerializer.ContentType {
			get { return "application/json"; }
		}
	}
}