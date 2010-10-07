using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public abstract class BinaryMessageSerializer : IMessageSerializer, ISystemMessageSerializer {
		private static readonly Dictionary<byte, Type> typeCodes;

		private Encoding encoding;

		protected BinaryMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected BinaryMessageSerializer()
			: this(null) {
		}

		static BinaryMessageSerializer() {
			typeCodes = new Dictionary<byte, Type>();
			typeCodes[0] = typeof(DBNull);

			typeCodes[1] = typeof(byte);
			typeCodes[2] = typeof(short);
			typeCodes[3] = typeof(int);
			typeCodes[4] = typeof(long);
			typeCodes[5] = typeof(float);
			typeCodes[6] = typeof(double);

			typeCodes[11] = typeof(DateTime);
			typeCodes[12] = typeof(TimeSpan);

			typeCodes[22] = typeof(char);
			typeCodes[23] = typeof(string);

			typeCodes[33] = typeof(bool);

			typeCodes[57] = typeof(Array);

			// extensions ...
			typeCodes[101] = typeof(IServiceAddress);
			typeCodes[102] = typeof(DataAddress);
			typeCodes[103] = typeof(NodeSet);
			typeCodes[104] = typeof(MessageError);
		}

		public Encoding Encoding {
			get {
				if (encoding == null)
					encoding = Encoding.UTF8;
				return encoding;
			}
			set { encoding = value; }
		}

		public static Type GetType(byte code) {
			Type type;
			if (typeCodes.TryGetValue(code, out type))
				return type;
			return null;
		}

		public static byte GetCode(Type type) {
			if (type.IsArray)
				return 57;
			
			foreach(KeyValuePair<byte, Type> pair in typeCodes) {
				if (pair.Value == type ||
				    (pair.Value.IsInterface && 
				     pair.Value.IsAssignableFrom(type)))
					return pair.Key;
			}

			throw new InvalidOperationException("The type '" + type + "' has no corresponding code: unhandled.");
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