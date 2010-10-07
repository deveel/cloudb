using System;
using System.Collections.Generic;
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

			if (message is MessageStream) {
				MessageStream messageStream = (MessageStream) message;

				writer.Write(2);
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

		public void Deserialize(Message message, Stream input) {
			if (!input.CanRead)
				throw new ArgumentException("The inpuit stream cannot be read.");

			BinaryReader reader = new BinaryReader(input, Encoding);
			int type = reader.ReadInt32();
			if (type == 1) {
				Deserialize(message, reader);
			} else if (type == 2) {
				if (!(message is MessageStream))
					throw new ArgumentException();

				MessageStream stream = (MessageStream) message;

				Message msg;
				if (stream.Type == MessageType.Request) {
					msg = new MessageRequest();
				} else {
					msg = new MessageResponse(null, null);
				}

				int sz = reader.ReadInt32();
				for (int i = 0; i < sz; i++) {
					Deserialize(msg, reader);
				}
			} else {
				throw new InvalidOperationException();
			}
		}

		protected abstract void Deserialize(Message message, BinaryReader input);

	}
}