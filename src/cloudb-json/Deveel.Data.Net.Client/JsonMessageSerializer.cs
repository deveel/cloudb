using System;
using System.IO;
using System.Text;

using Deveel.Data.Net.Serialization;

using Newtonsoft.Json;

namespace Deveel.Data.Net.Client {
	public abstract class JsonMessageSerializer : ITextMessageSerializer {
		private Encoding encoding;

		protected JsonMessageSerializer(string encoding)
			: this(!String.IsNullOrEmpty(encoding) ? Encoding.GetEncoding(encoding) : Encoding.UTF8) {
		}

		protected JsonMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected JsonMessageSerializer()
			: this(Encoding.UTF8) {
		}

		public Encoding ContentEncoding {
			get {
				if (encoding == null)
					encoding = Encoding.UTF8;
				return encoding;
			}
			set { encoding = value; }
		}

		public void Serialize(Message message, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");
			if (!output.CanWrite)
				throw new ArgumentException("The output stream is not writeable.");

			JsonTextWriter writer = new JsonTextWriter(new StreamWriter(output, ContentEncoding));
			Serialize(message, writer);
			writer.Flush();
		}

		protected abstract void Serialize(Message message, JsonTextWriter writer);

		public Message Deserialize(Stream input, MessageType messageType) {
			if (input == null)
				throw new ArgumentNullException("input");
			if (!input.CanRead)
				throw new ArgumentException("The input stream is not readable.");

			return Deserialize(new JsonTextReader(new StreamReader(input, ContentEncoding)), messageType);
		}

		protected abstract Message Deserialize(JsonTextReader reader, MessageType messageType);

		string ITextMessageSerializer.ContentEncoding {
			get { return ContentEncoding.BodyName; }
		}

		string ITextMessageSerializer.ContentType {
			get { return "application/json"; }
		}
	}
}