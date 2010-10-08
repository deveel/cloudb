using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public sealed class BinaryRpcMessageSerializer : BinaryMessageSerializer, IMessageStreamSupport {
		private static readonly Dictionary<byte, Type> typeCodes;

		public BinaryRpcMessageSerializer(Encoding encoding)
			: base(encoding) {
		}

		public BinaryRpcMessageSerializer()
			: this(null) {
		}
		
		static BinaryRpcMessageSerializer() {
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
		
		private static Type GetType(byte code) {
			Type type;
			if (typeCodes.TryGetValue(code, out type))
				return type;
			return null;
		}

		private static byte GetCode(Type type) {
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


		private static object ReadValue(BinaryReader reader) {
			byte typeCode = reader.ReadByte();
			return ReadValue(reader, typeCode);
		}

		private static object ReadValue(BinaryReader reader, byte typeCode) {
			Type type = GetType(typeCode);
			if (type == typeof(DBNull))
				return null;
			if (type == typeof(bool))
				return reader.ReadBoolean();
			if (type == typeof(byte))
				return reader.ReadByte();
			if (type == typeof(short))
				return reader.ReadInt16();
			if (type == typeof(int))
				return reader.ReadInt32();
			if (type == typeof(long))
				return reader.ReadInt64();
			if (type == typeof(float))
				return reader.ReadSingle();
			if (type == typeof(double))
				return reader.ReadDouble();
			if (type == typeof(DateTime))
				return DateTime.FromBinary(reader.ReadInt64());

			if (type == typeof(char))
				return reader.ReadChar();

			if (type == typeof(string)) {
				int sz = reader.ReadInt32();
				StringBuilder sb = new StringBuilder(sz);
				for (int i = 0; i < sz; i++) {
					sb.Append(reader.ReadChar());
				}
				return sb.ToString();
			}

			// Extensions ...

			if (type == typeof(IServiceAddress)) {
				int addressTypeCode = reader.ReadInt32();
				Type addressType = ServiceAddresses.GetAddressType(addressTypeCode);
				IServiceAddressHandler handler = ServiceAddresses.GetHandler(addressType);
				int length = reader.ReadInt32();
				byte[] buffer = reader.ReadBytes(length);
				return handler.FromBytes(buffer);
			}

			if (type == typeof(DataAddress)) {
				int dataId = reader.ReadInt32();
				long blockId = reader.ReadInt64();
				return new DataAddress(blockId, dataId);
			}

			if (type == typeof(MessageError)) {
				string source = reader.ReadString();
				string message = reader.ReadString();
				string stackTrace = reader.ReadString();
				return new MessageError(source, message, stackTrace);
			}

			if (type == typeof(NodeSet)) {
				byte node_set_type = reader.ReadByte();
				// The node_ids list,
				int sz = reader.ReadInt32();
				long[] arr = new long[sz];
				for (int n = 0; n < sz; ++n) {
					arr[n] = reader.ReadInt64();
				}
				// The binary encoding,
				sz = reader.ReadInt32();
				byte[] buf = new byte[sz];
				// Util.BinaryReader.ReadFully(din, buf, 0, sz);
				reader.Read(buf, 0, sz);
				// Make the node_set object type,
				if (node_set_type == 1)
					// Uncompressed single,
					return new SingleNodeSet(arr, buf);
				if (node_set_type == 2)
					// Compressed group,
					return new CompressedNodeSet(arr, buf);
				
				throw new Exception("Unknown node set type: " + node_set_type);
			}

			// array
			if (type == typeof(Array)) {
				byte arrayTypeCode = reader.ReadByte();
				int sz = reader.ReadInt32();
				Type arrayType = GetType(arrayTypeCode);
				Array array = Array.CreateInstance(arrayType, sz);
				for (int i = 0; i < sz; i++) {
					object value = ReadValue(reader, arrayTypeCode);
					array.SetValue(value, i);
				}

				return array;
			}

			throw new FormatException();
		}

		private static MessageArgument ReadArgument(BinaryReader reader) {
			string argName = reader.ReadString();
			object value = ReadValue(reader);

			MessageArgument argument = new MessageArgument(argName, value);

			int sz = reader.ReadInt32();
			for (int i = 0; i < sz; i++)
				argument.Children.Add(ReadArgument(reader));

			return argument;
		}

		private static void WriteValue(object value, BinaryWriter writer) {
			Type valueType = value == null ? typeof(DBNull) : value.GetType();
			byte valueTypeCode = GetCode(valueType);
			writer.Write(valueTypeCode);

			if (value == null) {

			} else if (valueType == typeof(byte)) {
				writer.Write((byte)value);
			} else if (valueType == typeof(short)) {
				writer.Write((short)value);
			} else if (valueType == typeof(int)) {
				writer.Write((int)value);
			} else if (valueType == typeof(long)) {
				writer.Write((long)value);
			} else if (valueType == typeof(float)) {
				writer.Write((float)value);
			} else if (valueType == typeof(double)) {
				writer.Write((double)value);
			} else if (valueType == typeof(bool)) {
				writer.Write((bool)value);
			} else if (valueType == typeof(DateTime)) {
				value = ((DateTime)value).ToBinary();
				writer.Write((long)value);
			} else if (valueType == typeof(TimeSpan)) {
				value = ((TimeSpan)value).Ticks;
				writer.Write((long)value);
			} else if (valueType == typeof(string)) {
				string s = (string) value;
				int sz = s.Length;
				writer.Write(sz);

				for (int i = 0; i < sz; i++) {
					writer.Write(s[i]);
				}
				// Extensions ...
			} else if (typeof(IServiceAddress).IsAssignableFrom(valueType)) {
				IServiceAddress address = (IServiceAddress) value;
				IServiceAddressHandler handler = ServiceAddresses.GetHandler(address);
				byte[] buffer = handler.ToBytes(address);
				int code = handler.GetCode(address.GetType());
				writer.Write(code);
				writer.Write(buffer.Length);
				writer.Write(buffer);
			} else if (valueType == typeof(DataAddress)) {
				DataAddress data_addr = (DataAddress) value;
				writer.Write(data_addr.DataId);
				writer.Write(data_addr.BlockId);
			} else if (valueType == typeof(MessageError)) {
				MessageError e = (MessageError) value;
				writer.Write(e.Source);
				writer.Write(e.Message);
				writer.Write(e.StackTrace);
			} else if (typeof(NodeSet).IsAssignableFrom(valueType)) {
				if (value is SingleNodeSet) {
					writer.Write((byte)1);
				} else if (value is CompressedNodeSet) {
					writer.Write((byte)2);
				} else {
					throw new Exception("Unknown NodeSet type: " + value.GetType());
				}
				NodeSet nset = (NodeSet)value;
				// Write the node set,
				// Write the binary encoding,
				nset.WriteTo(writer.BaseStream);
			} else if (value is Array) {
				Array array = (Array)value;
				Type arrayType = array.GetType().GetElementType();
				byte arrayTypeCode = GetCode(arrayType);

				int sz = array.Length;
				writer.Write(arrayTypeCode);
				writer.Write(sz);

				for (int i = 0; i < sz; i++) {
					object item = array.GetValue(i);
					WriteValue(item, writer);
				}
			}
		}

		private static void WriteArgument(MessageArgument argument, BinaryWriter writer) {
			writer.Write(argument.Name);
			WriteValue(argument.Value, writer);

			int sz = argument.Children.Count;
			writer.Write(sz);
			for (int i = 0; i < sz; i++) {
				WriteArgument(argument.Children[i], writer);
			}
		}

		protected override Message Deserialize(BinaryReader reader, MessageType messageType) {
			Message message;
			string messageName = reader.ReadString();
			if (messageType == MessageType.Request) {
				message = new RequestMessage(messageName);
			} else {
				message = new ResponseMessage(messageName);
			}

			int sz = reader.ReadInt32();
			for (int i = 0; i < sz; i++)
				message.Arguments.Add(ReadArgument(reader));

			int v = reader.ReadInt32();
			if (v != 8)
				throw new FormatException();
			
			return message;
		}

		protected override void Serialize(Message message, BinaryWriter writer) {
			writer.Write(message.Name);

			int sz = message.Arguments.Count;
			writer.Write(sz);
			for (int i = 0; i < sz; i++) {
				MessageArgument argument = message.Arguments[i];
				WriteArgument(argument, writer);
			}

			writer.Write(8);
		}
	}
}