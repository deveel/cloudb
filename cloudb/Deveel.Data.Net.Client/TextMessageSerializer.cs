using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public abstract class TextMessageSerializer : ITextMessageSerializer {
		private Encoding encoding;
		
		protected TextMessageSerializer(string encoding)
			: this(!String.IsNullOrEmpty(encoding) ? Encoding.GetEncoding(encoding) : Encoding.UTF8) {
		}

		protected TextMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected TextMessageSerializer()
			: this(Encoding.UTF8) {
		}


		string ITextMessageSerializer.ContentEncoding {
			get { return encoding.BodyName; }
		}
		
		public Encoding ContentEncoding {
			get {
				if (encoding == null)
					encoding = Encoding.UTF8;
				return encoding;
			}
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				encoding = value;
			}
		}

		
		protected abstract string ContentType { get; }
		
		string ITextMessageSerializer.ContentType {
			get { return ContentType; }
		}
		
		public Message Deserialize(Stream input, MessageType messageType) {
			if (input == null)
				throw new ArgumentNullException("input");
			if (!input.CanRead)
				throw new ArgumentException("The input stream cannot be read");

			return Deserialize(new StreamReader(input, ContentEncoding), messageType);
		}

		protected abstract Message Deserialize(TextReader reader, MessageType messageType);

		public void Serialize(Message message, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");
			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			TextWriter writer = new StreamWriter(output, ContentEncoding);
			Serialize(message, writer);
			writer.Flush();
		}

		protected abstract void Serialize(Message message, TextWriter writer);
	}
}