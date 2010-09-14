using System;
using System.IO;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net {
	public sealed class XmlMessageStreamSerializer : IMessageSerializer {
		private static int ReadInt(XmlReader reader) {
			string text = ReadString(reader);
			int value;
			if (!Int32.TryParse(text, out value))
				throw new FormatException("The content of the <int/> element is not valid.");

			return value;
		}

		private static int[] ReadIntArray(XmlReader reader) {
			return (int[]) ReadArray(reader, typeof(int));
		}

		private static long[] ReadLongArray(XmlReader reader) {
			return (long[]) ReadArray(reader, typeof(long));
		}

		private static long ReadLong(XmlReader reader) {
			string text = ReadString(reader);
			long value;
			if (!Int64.TryParse(text, out value))
				throw new FormatException("The content of the <long/> element is not valid.");

			return value;			
		}

		private static string ReadString(XmlReader reader) {
			string value = null;
			if (reader.Read()) {
				if (reader.NodeType != XmlNodeType.Text)
					throw new FormatException("Invalid contents of the <string/> element.");

				value = reader.Value;
			}

			return value;
		}

		private static Array ReadArray(XmlReader reader, Type type) {
			int elementCount = -1;
			Array array = null;
			int i = -1;

			while (reader.Read()) {
				XmlNodeType nodeType = reader.NodeType;
				if (nodeType == XmlNodeType.Attribute) {
					if (reader.LocalName != "length")
						throw new FormatException("Invalid attribute.");

					if (elementCount != -1)
						throw new FormatException();

					string text = reader.Value;
					if (!Int32.TryParse(text, out elementCount))
						throw new FormatException();

					array = Array.CreateInstance(type, elementCount);
				} else if (nodeType == XmlNodeType.Element) {
					if (array == null)
						throw new InvalidOperationException();

					object value = null;
					if (type == typeof(int))
						value = ReadInt(reader);
					else if (type == typeof(long))
						value = ReadLong(reader);
					else if (type == typeof(string))
						value = ReadString(reader);
					else if (type == typeof(IServiceAddress))
						value = ServiceAddresses.ParseString(ReadString(reader));

					array.SetValue(value, i++);
				}

				if (--elementCount == 0)
					break;
			}

			return array;
		}

		private static string [] ReadStringArray(XmlReader reader) {
			return (string[]) ReadArray(reader, typeof(string));
		}

		private static DataAddress ReadDataAddress(XmlReader reader) {
			bool blockIdFound = false;
			int blockId = -1;

			bool dataIdFound = false;
			int dataId = -1;

			while (reader.Read()) {
				XmlNodeType nodeType = reader.NodeType;
				if (nodeType != XmlNodeType.Attribute)
					throw new FormatException();

				string name = reader.LocalName;
				if (name == "blockId") {
					if (blockIdFound)
						throw new FormatException();

					if (!Int32.TryParse(reader.Value, out blockId))
						throw new FormatException();

					blockIdFound = true;
				} else if (name == "dataId") {
					if (dataIdFound)
						throw new FormatException();
					if (!Int32.TryParse(reader.Value, out dataId))
						throw new FormatException();

					dataIdFound = true;
				} else {
					throw new FormatException();
				}
			}

			if (!blockIdFound || !dataIdFound)
				throw new FormatException("Either blockId or dataId values not found.");

			return new DataAddress(blockId, dataId);
		}

		private static IServiceAddress ReadServiceAddress(XmlReader reader) {
			//TODO: this method is not very performant ...

			string s = ReadString(reader);
			try {
				return ServiceAddresses.ParseString(s);
			} catch(Exception) {
				throw new FormatException("Unable to parse the address '" + s + "'.");
			}
		}

		private static IServiceAddress[] ReadServiceAddressArray(XmlReader reader) {
			return (IServiceAddress[]) ReadArray(reader, typeof(IServiceAddress));
		}

		public MessageStream Deserialize(Stream input) {
			//TODO: load a XML schema to validate the input?
			XmlReader reader = new XmlTextReader(input);

			MessageStream messageStream = new MessageStream(16);

			bool messageStart = false;

			while (reader.Read()) {
				XmlNodeType nodeType = reader.NodeType;
				//TODO: should we take the 'encoding' attribute?
				if (nodeType == XmlNodeType.XmlDeclaration)
					continue;

				if (nodeType == XmlNodeType.Comment ||
					nodeType == XmlNodeType.Whitespace)
					continue;

				if (nodeType == XmlNodeType.Element) {
					string elementName = reader.LocalName;
					object value;
					if (elementName == "message") {
						messageStart = true;
						continue;
					}
					if (elementName == "null") {
						value = null;
					} else if (elementName == "int") {
						value = ReadInt(reader);
					} else if (elementName == "intArray") {
						value = ReadIntArray(reader);
					} else if (elementName == "long") {
						value = ReadLong(reader);
					} else if (elementName == "byteArray") {
						value = ReadByteArray(reader);
					} else if (elementName == "string") {
						value = ReadString(reader);
					} else if (elementName == "stringArray") {
						value = ReadStringArray(reader);
					} else if (elementName == "longArray") {
						value = ReadLongArray(reader);
					} else if (elementName == "nodeSet") {
						value = ReadNodeSet(reader);
					} else if (elementName == "dataAddress") {
						value = ReadDataAddress(reader);
					} else if (elementName == "error") {
						value = ReadError(reader);
					} else if (elementName == "serviceAddressArray") {
						value = ReadServiceAddressArray(reader);
					} else if (elementName == "serviceAddress") {
						value = ReadServiceAddress(reader);
					} else {
						throw new FormatException("Invalid element '" + elementName + "' found in the stream.");
					}

					messageStream.AddMessageArgument(value);
				} else if (nodeType == XmlNodeType.EndElement) {
					string elementName = reader.LocalName;
					if (elementName == "message")
						messageStream.CloseMessage();
				} else if (nodeType == XmlNodeType.Attribute) {
					if (messageStart) {
						if (reader.LocalName != "name")
							throw new FormatException();

						messageStream.StartMessage(reader.Value);
						messageStart = false;
					}
				}

			}

			throw new NotImplementedException();
		}

		private static ServiceException ReadError(XmlReader reader) {
			throw new NotImplementedException();
		}

		private static NodeSet ReadNodeSet(XmlReader reader) {
			throw new NotImplementedException();
		}

		private static byte[ ]ReadByteArray(XmlReader reader) {
			throw new NotImplementedException();
		}

		public void Serialize(MessageStream messageStream, Stream output) {
			throw new NotImplementedException();
		}
	}
}