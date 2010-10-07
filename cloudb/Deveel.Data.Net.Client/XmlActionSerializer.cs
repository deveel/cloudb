using System;
using System.IO;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public abstract class XmlMessageSerializer : ITextMessageSerializer {
		private Encoding encoding;

		protected XmlMessageSerializer(string encoding)
			: this(!String.IsNullOrEmpty(encoding) ? Encoding.GetEncoding(encoding) : Encoding.UTF8) {
		}

		protected XmlMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected XmlMessageSerializer()
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

		string ITextMessageSerializer.ContentType {
			get { return "text/xml"; }
		}

		public void Deserialize(Message message, Stream input) {
			if (input == null)
				throw new ArgumentNullException("input");
			if (!input.CanRead)
				throw new ArgumentException("The input stream cannot be read");

			Deserialize(message, new XmlTextReader(new StreamReader(input, ContentEncoding)));
		}

		protected abstract void Deserialize(Message message, XmlReader reader);

		public void Serialize(Message message, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");
			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			XmlTextWriter writer = new XmlTextWriter(new StreamWriter(output, ContentEncoding));
			Serialize(message, writer);
			writer.Flush();
		}

		protected abstract void Serialize(Message message, XmlWriter writer);
	}
}