using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public sealed class BinaryRpcActionSerializer : IActionSerializer {
		private Encoding encoding;

		public BinaryRpcActionSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		public BinaryRpcActionSerializer()
			: this(null) {
		}

		public Encoding Encoding {
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
				throw new ArgumentException("The input stream cannot be read.");

			DeserializeRequest(request, new BinaryReader(input, Encoding));
		}

		public void DeserializeRequest(ActionRequest request, BinaryReader reader) {
			throw new NotImplementedException();
		}

		public void SerializeResponse(ActionResponse response, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");
			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			BinaryWriter writer = new BinaryWriter(output, Encoding);
			SerializeResponse(response, writer);
			writer.Flush();
		}

		public void SerializeResponse(ActionResponse response, BinaryWriter writer) {
			
		}
	}
}