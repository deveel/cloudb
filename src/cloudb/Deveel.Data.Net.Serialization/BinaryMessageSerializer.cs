using System;
using System.IO;
using System.Text;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net.Serialization {
	public abstract class BinaryMessageSerializer : IMessageSerializer {
		private Encoding encoding;

		protected BinaryMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected BinaryMessageSerializer()
			: this(null) {
		}

		public Encoding Encoding {
			get { return encoding ?? (encoding = Encoding.UTF8); }
			set { encoding = value; }
		}

		public void Serialize(Message message, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");

			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			BinaryWriter writer = new BinaryWriter(output, Encoding);
			Serialize(message, writer);
		}

		protected abstract void Serialize(Message message, BinaryWriter writer);

		public Message Deserialize(Stream input, MessageType messageType) {
			if (!input.CanRead)
				throw new ArgumentException("The inpuit stream cannot be read.");

			BinaryReader reader = new BinaryReader(input, Encoding);
			return Deserialize(reader, messageType);
		}

		protected abstract Message Deserialize(BinaryReader input, MessageType messageType);

	}
}