using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public abstract class JsonActionSerializer : ITextActionSerializer {
		private Encoding encoding;

		protected JsonActionSerializer(string encoding)
			: this(!String.IsNullOrEmpty(encoding) ? Encoding.GetEncoding(encoding) : Encoding.UTF8) {
		}

		protected JsonActionSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected JsonActionSerializer()
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

		public void DeserializeRequest(ActionRequest request, Stream input) {
			if (input == null)
				throw new ArgumentNullException("input");
			if (!input.CanRead)
				throw new ArgumentException("The input stream is not readable.");

			DeserializeRequest(request, new StreamReader(input, ContentEncoding));
		}

		public abstract void DeserializeRequest(ActionRequest request, TextReader reader);

		public void SerializeResponse(ActionResponse response, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");
			if (!output.CanWrite)
				throw new ArgumentException("The output stream is not writeable.");

			StreamWriter writer = new StreamWriter(output, ContentEncoding);
			SerializeResponse(response, writer);
			writer.Flush();
		}

		public abstract void SerializeResponse(ActionResponse response, TextWriter writer);

		string ITextActionSerializer.ContentEncoding {
			get { return ContentEncoding.BodyName; }
		}

		string ITextActionSerializer.ContentType {
			get { return "application/json"; }
		}
	}
}