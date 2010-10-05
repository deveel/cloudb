using System;
using System.IO;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public abstract class XmlActionSerializer : ITextActionSerializer {
		private Encoding encoding;

		protected XmlActionSerializer(string encoding)
			: this(!String.IsNullOrEmpty(encoding) ? Encoding.GetEncoding(encoding) : Encoding.UTF8) {
		}

		protected XmlActionSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected XmlActionSerializer()
			: this(Encoding.UTF8) {
		}

		string ITextActionSerializer.ContentEncoding {
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

		string ITextActionSerializer.ContentType {
			get { return "text/xml"; }
		}

		public void DeserializeRequest(ActionRequest request, Stream input) {
			if (input == null)
				throw new ArgumentNullException("input");
			if (!input.CanRead)
				throw new ArgumentException("The input stream cannot be read");

			DeserializeRequest(request, new XmlTextReader(new StreamReader(input, ContentEncoding)));
		}

		public abstract void DeserializeRequest(ActionRequest request, XmlReader reader);

		public void SerializeResponse(ActionResponse response, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");
			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			XmlTextWriter writer = new XmlTextWriter(new StreamWriter(output, ContentEncoding));
			SerializeResponse(response, writer);
			writer.Flush();
		}

		public abstract void SerializeResponse(ActionResponse response, XmlWriter writer);
	}
}