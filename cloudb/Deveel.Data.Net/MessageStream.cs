using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class MessageStream : IEnumerable<Message> {
		public MessageStream(int size) {
			items = new List<object>(size);
		}

		private readonly List<object> items;

		private const string MessageOpen = "[";
		private const string MessageClose = "]";

		public void StartMessage(string messageName) {
			items.Add(messageName);
			items.Add(MessageOpen);
		}

		public void CloseMessage() {
			items.Add(MessageClose);
		}

		public void AddErrorMessage(ServiceException error) {
			StartMessage("E");
			AddMessageArgument(error);
			CloseMessage();
		}

		public void AddMessageArgument(object value) {
			items.Add(value);
		}

		public void AddMessage(Message message) {
			StartMessage(message.Name);
			for (int i = 0; i < message.ArgumentCount; i++) {
				AddMessageArgument(message[i]);
			}
			CloseMessage();
		}

		internal void WriteTo(Stream output) {
			BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);
			writer.Write(items.Count);
			foreach (object item in items) {
				// Null value handling,
				if (item == null) {
					writer.Write((byte)16);
				} else if (item is String) {
					if (item.Equals(MessageOpen))
						continue;
					if (item.Equals(MessageClose)) {
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
				} else if (item is StringArgument) {
					writer.Write((byte)5);
					StringArgument str_arg = (StringArgument)item;
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
					nset.WriteTo(output);
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
				} else if (item is ServiceAddress[]) {
					writer.Write((byte)11);
					ServiceAddress[] arr = (ServiceAddress[])item;
					writer.Write(arr.Length);
					foreach (ServiceAddress s in arr) {
						s.WriteTo(writer);
					}
				} else if (item is DataAddress[]) {
					writer.Write((byte)12);
					DataAddress[] arr = (DataAddress[])item;
					writer.Write(arr.Length);
					foreach (DataAddress addr in arr) {
						writer.Write(addr.DataId);
						writer.Write(addr.BlockId);
					}
				} else if (item is ServiceAddress) {
					writer.Write((byte)13);
					((ServiceAddress)item).WriteTo(writer);
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

		internal static MessageStream ReadFrom(Stream input) {
			BinaryReader reader = new BinaryReader(input, Encoding.Unicode);
			int message_sz = reader.ReadInt32();
			MessageStream message_str = new MessageStream(message_sz);
			for (int i = 0; i < message_sz; ++i) {
				byte type = reader.ReadByte();
				if (type == 16) {
					// Nulls
					message_str.AddMessageArgument(null);
				} else if (type == 1) {
					// Open message,
					string message_name = reader.ReadString();
					message_str.StartMessage(message_name);
				} else if (type == 2) {
					message_str.AddMessageArgument(reader.ReadInt64());
				} else if (type == 3) {
					message_str.AddMessageArgument(reader.ReadInt32());
				} else if (type == 4) {
					int sz = reader.ReadInt32();
					byte[] buf = new byte[sz];
					reader.Read(buf, 0, sz);
					message_str.AddMessageArgument(buf);
				} else if (type == 5) {
					message_str.AddMessageArgument(reader.ReadString());
				} else if (type == 6) {
					// Long array
					int sz = reader.ReadInt32();
					long[] arr = new long[sz];
					for (int n = 0; n < sz; ++n) {
						arr[n] = reader.ReadInt64();
					}
					message_str.AddMessageArgument(arr);
				} else if (type == 7) {
					message_str.CloseMessage();
				} else if (type == 9) {
					int data_id = reader.ReadInt32();
					long block_id = reader.ReadInt64();
					message_str.AddMessageArgument(new DataAddress(block_id, data_id));
				} else if (type == 10) {
					string source = reader.ReadString();
					string message = reader.ReadString();
					string stackTrace = reader.ReadString();
					message_str.AddMessageArgument(new ServiceException(source, message, stackTrace));
				} else if (type == 11) {
					int sz = reader.ReadInt32();
					ServiceAddress[] arr = new ServiceAddress[sz];
					for (int n = 0; n < sz; ++n) {
						arr[n] = ServiceAddress.ReadFrom(reader);
					}
					message_str.AddMessageArgument(arr);
				} else if (type == 12) {
					int sz = reader.ReadInt32();
					DataAddress[] arr = new DataAddress[sz];
					for (int n = 0; n < sz; ++n) {
						int data_id = reader.ReadInt32();
						long block_id = reader.ReadInt64();
						arr[n] = new DataAddress(block_id, data_id);
					}
					message_str.AddMessageArgument(arr);
				} else if (type == 13) {
					message_str.AddMessageArgument(ServiceAddress.ReadFrom(reader));
				} else if (type == 14) {
					int sz = reader.ReadInt32();
					String[] arr = new String[sz];
					for (int n = 0; n < sz; ++n) {
						String str = reader.ReadString();
						arr[n] = str;
					}
					message_str.AddMessageArgument(arr);
				} else if (type == 15) {
					int sz = reader.ReadInt32();
					int[] arr = new int[sz];
					for (int n = 0; n < sz; ++n) {
						arr[n] = reader.ReadInt32();
					}
					message_str.AddMessageArgument(arr);
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
						message_str.AddMessageArgument(new SingleNodeSet(arr, buf));
					} else if (node_set_type == 2) {
						// Compressed group,
						message_str.AddMessageArgument(new CompressedNodeSet(arr, buf));
					} else {
						throw new Exception("Unknown node set type: " + node_set_type);
					}
				} else {
					throw new Exception("Unknown message type on stream " + type);
				}
			}
			// Consume the last byte type,
			byte v = reader.ReadByte();
			if (v != 8) {
				throw new Exception("Expected '8' to end message stream");
			}
			// Return the message str
			return message_str;
		}


		#region Implementation of IEnumerable

		public IEnumerator<Message> GetEnumerator() {
			return new MessageEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#endregion

		#region MessageEnumerator

		private class MessageEnumerator : IEnumerator<Message> {
			public MessageEnumerator(MessageStream stream) {
				this.stream = stream;
			}

			private readonly MessageStream stream;
			private int index = -1;

			#region Implementation of IDisposable

			public void Dispose() {
			}

			#endregion

			#region Implementation of IEnumerator

			public bool MoveNext() {
				return ++index < stream.items.Count;
			}

			public void Reset() {
				index = -1;
			}

			public Message Current {
				get {
					string msgName = (string)stream.items[index];
					Message message = msgName.Equals("E") ? new ErrorMessage() : new Message(msgName);
					while (++index < stream.items.Count) {
						object v = stream.items[index];
						if (v == null) {
							message.AddArgument(v);
						} else if (v is string) {
							if (v.Equals(MessageOpen))
								continue;
							if (v.Equals(MessageClose))
								return message;
							throw new FormatException("Invalid message format");
						} else if (v is StringArgument) {
							message.AddArgument(((StringArgument)v).Value);
						} else {
							message.AddArgument(v);
						}
					}

					throw new FormatException("No termination found in the message.");
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			#endregion
		}

		#endregion

		#region StringArgument

		private class StringArgument {
			public StringArgument(string value) {
				Value = value;
			}

			public readonly string Value;
		}

		#endregion
	}
}