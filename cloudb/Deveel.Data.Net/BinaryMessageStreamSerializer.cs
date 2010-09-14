using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class BinaryMessageStreamSerializer : IMessageSerializer {
		public void Serialize(MessageStream messageStream, BinaryWriter writer) {
			if (messageStream == null)
				throw new ArgumentNullException("messageStream");

			writer.Write(messageStream.Items.Count);
			foreach (object item in messageStream.Items) {
				// Null value handling,
				if (item == null) {
					writer.Write((byte)16);
				} else if (item is String) {
					if (item.Equals(MessageStream.MessageClose)) {
						writer.Write((byte)7);
					} else {
						writer.Write((byte)1);
						string str_msg = (string)item;
						writer.Write(str_msg);
					}
				} else if (item is long) {
					writer.Write((byte)2);
					writer.Write((long)item);
				} else if (item is int) {
					writer.Write((byte)3);
					writer.Write((int)item);
				} else if (item is byte[]) {
					writer.Write((byte)4);
					byte[] buf = (byte[])item;
					writer.Write(buf.Length);
					writer.Write(buf);
				} else if (item is MessageStream.StringArgument) {
					writer.Write((byte)5);
					MessageStream.StringArgument str_arg = (MessageStream.StringArgument)item;
					writer.Write(str_arg.Value);
				} else if (item is long[]) {
					writer.Write((byte)6);
					long[] arr = (long[])item;
					writer.Write(arr.Length);
					for (int i = 0; i < arr.Length; ++i) {
						writer.Write(arr[i]);
					}
				} else if (item is NodeSet) {
					writer.Write((byte)17);
					if (item is SingleNodeSet) {
						writer.Write((byte)1);
					} else if (item is CompressedNodeSet) {
						writer.Write((byte)2);
					} else {
						throw new Exception("Unknown NodeSet type: " + item.GetType());
					}
					NodeSet nset = (NodeSet)item;
					// Write the node set,
					// Write the binary encoding,
					nset.WriteTo(writer.BaseStream);
				} else if (item is DataAddress) {
					writer.Write((byte)9);
					DataAddress data_addr = (DataAddress)item;
					writer.Write(data_addr.DataId);
					writer.Write(data_addr.BlockId);
				} else if (item is ServiceException) {
					writer.Write((byte)10);
					ServiceException e = (ServiceException)item;
					writer.Write(e.Source);
					writer.Write(e.Message);
					writer.Write(e.StackTrace);
				} else if (item is IServiceAddress[]) {
					writer.Write((byte)11);
					IServiceAddress[] arr = (IServiceAddress[])item;
					writer.Write(arr.Length);
					foreach (IServiceAddress s in arr) {
						IServiceAddressHandler handler = ServiceAddresses.GetHandler(s);
						byte[] buffer = handler.ToBytes(s);
						int code = handler.GetCode(s.GetType());
						writer.Write(code);
						writer.Write(buffer.Length);
						writer.Write(buffer);
					}
				} else if (item is DataAddress[]) {
					writer.Write((byte)12);
					DataAddress[] arr = (DataAddress[])item;
					writer.Write(arr.Length);
					foreach (DataAddress addr in arr) {
						writer.Write(addr.DataId);
						writer.Write(addr.BlockId);
					}
				} else if (item is IServiceAddress) {
					writer.Write((byte)13);
					IServiceAddress address = (IServiceAddress)item;
					IServiceAddressHandler handler = ServiceAddresses.GetHandler(address);
					byte[] buffer = handler.ToBytes(address);
					int code = handler.GetCode(address.GetType());
					writer.Write(code);
					writer.Write(buffer.Length);
					writer.Write(buffer);
				} else if (item is String[]) {
					writer.Write((byte)14);
					String[] arr = (String[])item;
					writer.Write(arr.Length);
					foreach (String s in arr) {
						writer.Write(s);
					}
				} else if (item is int[]) {
					writer.Write((byte)15);
					int[] arr = (int[])item;
					writer.Write(arr.Length);
					foreach (int v in arr) {
						writer.Write(v);
					}
				} else {
					throw new ArgumentException("Unknown message object in list");
				}
			}
			// End of stream (for now).
			writer.Write((byte)8);
		}

		public void Serialize(MessageStream messageStream, Stream outputStream) {
			if (outputStream == null)
				throw new ArgumentNullException("outputStream");
			if (!outputStream.CanWrite)
				throw new ArgumentException("The output stream is not writeable");

			BinaryWriter writer = new BinaryWriter(outputStream, Encoding.Unicode);
			Serialize(messageStream, writer);
		}

		public MessageStream Deserialize(BinaryReader reader) {
			int messageSz = reader.ReadInt32();
			MessageStream messageStream = new MessageStream(messageSz);
			for (int i = 0; i < messageSz; ++i) {
				byte type = reader.ReadByte();
				if (type == 16) {
					// Nulls
					messageStream.AddMessageArgument(null);
				} else if (type == 1) {
					// Open message,
					string messageName = reader.ReadString();
					messageStream.StartMessage(messageName);
				} else if (type == 2) {
					messageStream.AddMessageArgument(reader.ReadInt64());
				} else if (type == 3) {
					messageStream.AddMessageArgument(reader.ReadInt32());
				} else if (type == 4) {
					int sz = reader.ReadInt32();
					byte[] buf = new byte[sz];
					reader.Read(buf, 0, sz);
					messageStream.AddMessageArgument(buf);
				} else if (type == 5) {
					messageStream.AddMessageArgument(reader.ReadString());
				} else if (type == 6) {
					// Long array
					int sz = reader.ReadInt32();
					long[] arr = new long[sz];
					for (int n = 0; n < sz; ++n) {
						arr[n] = reader.ReadInt64();
					}
					messageStream.AddMessageArgument(arr);
				} else if (type == 7) {
					messageStream.CloseMessage();
				} else if (type == 9) {
					int data_id = reader.ReadInt32();
					long block_id = reader.ReadInt64();
					messageStream.AddMessageArgument(new DataAddress(block_id, data_id));
				} else if (type == 10) {
					string source = reader.ReadString();
					string message = reader.ReadString();
					string stackTrace = reader.ReadString();
					messageStream.AddMessageArgument(new ServiceException(source, message, stackTrace));
				} else if (type == 11) {
					int sz = reader.ReadInt32();
					IServiceAddress[] arr = new IServiceAddress[sz];
					for (int n = 0; n < sz; ++n) {
						int typeCode = reader.ReadInt32();
						Type addressType = ServiceAddresses.GetAddressType(typeCode);
						IServiceAddressHandler handler = ServiceAddresses.GetHandler(addressType);						
						int length = reader.ReadInt32();
						byte[] buffer = reader.ReadBytes(length);
						arr[n] = handler.FromBytes(buffer);
					}
					messageStream.AddMessageArgument(arr);
				} else if (type == 12) {
					int sz = reader.ReadInt32();
					DataAddress[] arr = new DataAddress[sz];
					for (int n = 0; n < sz; ++n) {
						int data_id = reader.ReadInt32();
						long block_id = reader.ReadInt64();
						arr[n] = new DataAddress(block_id, data_id);
					}
					messageStream.AddMessageArgument(arr);
				} else if (type == 13) {
					int typeCode = reader.ReadInt32();
					Type addressType = ServiceAddresses.GetAddressType(typeCode);
					IServiceAddressHandler handler = ServiceAddresses.GetHandler(addressType);						
					int length = reader.ReadInt32();
					byte[] buffer = reader.ReadBytes(length);
					messageStream.AddMessageArgument(handler.FromBytes(buffer));
				} else if (type == 14) {
					int sz = reader.ReadInt32();
					String[] arr = new String[sz];
					for (int n = 0; n < sz; ++n) {
						String str = reader.ReadString();
						arr[n] = str;
					}
					messageStream.AddMessageArgument(arr);
				} else if (type == 15) {
					int sz = reader.ReadInt32();
					int[] arr = new int[sz];
					for (int n = 0; n < sz; ++n) {
						arr[n] = reader.ReadInt32();
					}
					messageStream.AddMessageArgument(arr);
				} else if (type == 17) {
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
					if (node_set_type == 1) {
						// Uncompressed single,
						messageStream.AddMessageArgument(new SingleNodeSet(arr, buf));
					} else if (node_set_type == 2) {
						// Compressed group,
						messageStream.AddMessageArgument(new CompressedNodeSet(arr, buf));
					} else {
						throw new Exception("Unknown node set type: " + node_set_type);
					}
				} else {
					throw new Exception("Unknown message type on stream " + type);
				}
			}

			// Consume the last byte type,
			byte v = reader.ReadByte();
			if (v != 8)
				throw new Exception("Expected '8' to end message stream");

			return messageStream;
		}

		public MessageStream Deserialize(Stream inputStream) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");
			if (!inputStream.CanRead)
				throw new ArgumentException("The input stream is not readable.", "inputStream");

			BinaryReader reader = new BinaryReader(inputStream, Encoding.Unicode);
			return Deserialize(reader);
		}
	}
}