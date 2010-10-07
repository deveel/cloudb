using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public abstract class BinaryMessageSerializer : IMessageSerializer, IMessageStreamSupport {
		private Encoding encoding;

		protected BinaryMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected BinaryMessageSerializer()
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

		public void Serialize(Message message, Stream output) {
			BinaryWriter writer = new BinaryWriter(output, Encoding);

			if (message is IMessageStream) {
				IMessageStream messageStream = (IMessageStream) message;

				if (messageStream.Type ==MessageType.Request)
					writer.Write(2);
				else
					writer.Write(3);

				int sz = messageStream.MessageCount;
				writer.Write(sz);

				foreach(Message child in messageStream) {
					Serialize(child, writer);
				}
			} else {
				writer.Write(1);
				Serialize(message, writer);
			}
		}

		protected abstract void Serialize(Message message, BinaryWriter writer);

		public Message Deserialize(Stream input) {
			if (!input.CanRead)
				throw new ArgumentException("The inpuit stream cannot be read.");

			BinaryReader reader = new BinaryReader(input, Encoding);
			int type = reader.ReadInt32();
			if (type == 1)
				return Deserialize(reader);
			if (type == 2 || type == 3) {
				IMessageStream stream;
				if (type == 2)
					stream = new RequestMessageStream();
				else
					stream = new ResponseMessageStream();

				int sz = reader.ReadInt32();
				for (int i = 0; i < sz; i++) {
					stream.AddMessage(Deserialize(reader));
				}

				return stream as Message;
			}
				
			throw new InvalidOperationException();
		}

		protected abstract Message Deserialize(BinaryReader input);

	}
}