using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Messaging {
	public abstract class BinaryMessageSerializer : IMessageSerializer {
		private Encoding encoding;

		protected BinaryMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected BinaryMessageSerializer()
			: this(null) {
		}

		public Encoding Encoding {
			get { return encoding ?? (encoding = Encoding.Unicode); }
			set { encoding = value; }
		}

		public void Serialize(IEnumerable<Message> message, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");

			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			BinaryWriter writer = new BinaryWriter(output, Encoding);
			Serialize(message, writer);
		}

		protected abstract void Serialize(IEnumerable<Message> message, BinaryWriter writer);

		public IEnumerable<Message> Deserialize(Stream input) {
			if (!input.CanRead)
				throw new ArgumentException("The inpuit stream cannot be read.");

			BinaryReader reader = new BinaryReader(input, Encoding);
			return Deserialize(reader);
		}

		protected abstract IEnumerable<Message> Deserialize(BinaryReader input);

	}
}