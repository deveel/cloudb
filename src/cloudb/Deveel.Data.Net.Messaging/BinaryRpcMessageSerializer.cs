//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Messaging {
	public sealed class BinaryRpcMessageSerializer : BinaryMessageSerializer {
		private static readonly Dictionary<byte, Type> TypeCodes;

		public BinaryRpcMessageSerializer(Encoding encoding)
			: base(encoding) {
		}

		public BinaryRpcMessageSerializer()
			: this(null) {
		}
		
		static BinaryRpcMessageSerializer() {
			TypeCodes = new Dictionary<byte, Type>();
			TypeCodes[0] = typeof(DBNull);

			TypeCodes[1] = typeof(byte);
			TypeCodes[2] = typeof(short);
			TypeCodes[3] = typeof(int);
			TypeCodes[4] = typeof(long);
			TypeCodes[5] = typeof(float);
			TypeCodes[6] = typeof(double);

			TypeCodes[11] = typeof(DateTime);
			TypeCodes[12] = typeof(TimeSpan);

			TypeCodes[22] = typeof(char);
			TypeCodes[23] = typeof(string);

			TypeCodes[33] = typeof(bool);

			TypeCodes[57] = typeof(Array);

			// extensions ...
			TypeCodes[101] = typeof(IServiceAddress);
			TypeCodes[102] = typeof(DataAddress);
			TypeCodes[103] = typeof(NodeSet);
			TypeCodes[104] = typeof(MessageError);
		}

		public bool SupportsMessageStream {
			get { return true; }
		}
		
		private static Type GetType(byte code) {
			Type type;
			if (TypeCodes.TryGetValue(code, out type))
				return type;
			return null;
		}

		private static byte GetCode(Type type) {
			if (type.IsArray)
				return 57;
			
			foreach(KeyValuePair<byte, Type> pair in TypeCodes) {
				if (pair.Value == type ||
				    ((pair.Value.IsInterface || pair.Value.IsAbstract) && 
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
				long blockIdHigh = reader.ReadInt64();
				long blockIdLow = reader.ReadInt64();
				return new DataAddress(new BlockId(blockIdHigh, blockIdLow), dataId);
			}

			if (type == typeof(MessageError)) {
				string source = reader.ReadString();
				string message = reader.ReadString();
				string stackTrace = reader.ReadString();
				string errorType = reader.ReadString();
				return new MessageError(source, message, stackTrace, errorType);
			}

			if (type == typeof(NodeSet)) {
				byte nodeSetType = reader.ReadByte();
				// The node_ids list,
				int sz = reader.ReadInt32();
				NodeId[] arr = new NodeId[sz];
				for (int n = 0; n < sz; ++n) {
					long nrHigh = reader.ReadInt64();
					long nrLow = reader.ReadInt64();
					arr[n] = new NodeId(nrHigh, nrLow);
				}
				// The binary encoding,
				sz = reader.ReadInt32();
				byte[] buf = new byte[sz];
				// Util.BinaryReader.ReadFully(din, buf, 0, sz);
				reader.Read(buf, 0, sz);
				// Make the node_set object type,
				if (nodeSetType == 1)
					// Uncompressed single,
					return new SingleNodeSet(arr, buf);
				if (nodeSetType == 2)
					// Compressed group,
					return new CompressedNodeSet(arr, buf);
				
				throw new Exception("Unknown node set type: " + nodeSetType);
			}

			// array
			if (type == typeof(Array)) {
				byte arrayTypeCode = reader.ReadByte();
				int sz = reader.ReadInt32();
				Type arrayType = GetType(arrayTypeCode);
				Array array = Array.CreateInstance(arrayType, sz);
				for (int i = 0; i < sz; i++) {
					object value = ReadValue(reader);
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
				DataAddress dataAddr = (DataAddress) value;
				writer.Write(dataAddr.DataId);
				writer.Write(dataAddr.BlockId.High);
				writer.Write(dataAddr.BlockId.Low);
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
				NodeId[] nodeIds = nset.NodeIds;
				int nsz = nodeIds.Length;
				writer.Write(nsz);
				for (int i = 0; i < nsz; i++) {
					NodeId nodeId = nodeIds[i];
					writer.Write(nodeId.High);
					writer.Write(nodeId.Low);
				}
				// Write the binary encoding,
				byte[] buffer = nset.Buffer;
				int bsz = buffer.Length;
				writer.Write(bsz);
				writer.Write(buffer, 0, bsz);
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

		private Message DeserializeMessage(BinaryReader reader) {
			string messageName = reader.ReadString();
			Message	message = new Message(messageName);

			int sz = reader.ReadInt32();
			for (int i = 0; i < sz; i++)
				message.Arguments.Add(ReadArgument(reader));

			int v = reader.ReadInt32();
			if (v != 8)
				throw new FormatException();

			return message;
		}

		protected override IEnumerable<Message> Deserialize(BinaryReader reader) {
			int sz = reader.ReadInt32();

			MessageStream stream = new MessageStream();
			for (int i = 0; i < sz; i++) {
				stream.AddMessage(DeserializeMessage(reader));
			}

			return stream;
		}

		private void SerializeMessage(Message message, BinaryWriter writer) {
			string messageName = message.Name;
			writer.Write(messageName);

			int sz = message.Arguments.Count;
			writer.Write(sz);
			foreach (MessageArgument argument in message.Arguments) {
				WriteArgument(argument, writer);
			}

			writer.Write(8);
		}

		protected override void Serialize(IEnumerable<Message> message, BinaryWriter writer) {
			List<Message> messages = new List<Message>(message);
			writer.Write(messages.Count);
			foreach (Message child in messages) {
				SerializeMessage(child, writer);
			}
		}
	}
}