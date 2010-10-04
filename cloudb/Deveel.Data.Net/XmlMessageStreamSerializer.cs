using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net {
	public sealed class XmlMessageStreamSerializer : ITextMessageSerializer {
		private Encoding encoding;
		
		public XmlMessageStreamSerializer(string encoding)
			: this(Encoding.GetEncoding(encoding)) {
		}
		
		public XmlMessageStreamSerializer(Encoding encoding) {
			this.encoding = encoding;
		}
		
		public XmlMessageStreamSerializer()
			: this(Encoding.UTF8) {
		}
		
		string ITextMessageSerializer.ContentType {
			get { return "text/xml"; }
		}
		
		string ITextMessageSerializer.ContentEncoding {
			get { return ContentEncoding.BodyName; }
		}
		
		public Encoding ContentEncoding {
			get { 
				if (encoding == null)
					encoding = Encoding.UTF8;
				return encoding;
			}
			set { encoding = value; }
		}
		
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
			StreamReader reader = new StreamReader(input, ContentEncoding);
			XmlReader xmlReader = new XmlTextReader(reader);
			return Deserialize(xmlReader);
		}

		public MessageStream Deserialize(XmlReader reader) {
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
					
					if (elementName == "stream")
						continue;
					
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
					} else if (elementName == "binary") {
						value = ReadBinary(reader);
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
					if (elementName == "message") {
						messageStream.CloseMessage();
					} else if (elementName == "stream") {
						break;
					}
				} else if (nodeType == XmlNodeType.Attribute) {
					if (messageStart) {
						if (reader.LocalName != "name")
							throw new FormatException();

						messageStream.StartMessage(reader.Value);
						messageStart = false;
					}
				}

			}

			return messageStream;
		}

		private static ServiceException ReadError(XmlReader reader) {
			throw new NotImplementedException();
		}

		private static NodeSet ReadNodeSet(XmlReader reader) {
			int nodeSetType = 1;
			long[] nodes = null;
			byte[] data = null;
			
			while (reader.Read()) {
				XmlNodeType nodeType = reader.NodeType;
				if (nodeType == XmlNodeType.Attribute) {
					if (reader.LocalName != "type")
						throw new FormatException();
					if (!Int32.TryParse(reader.Value, out nodeSetType))
						throw new FormatException();
				}
				if (nodeType != XmlNodeType.Element)
					continue;
				
				if (reader.LocalName == "nodes") {				
					nodes = ReadLongArray(reader);
				} else if (reader.LocalName == "data") {
					data = ReadBinary(reader);
				}
			}
			
			if (nodes == null || data == null)
				throw new FormatException();
			
			if (nodeSetType == 1)
				return new SingleNodeSet(nodes, data);
			else if (nodeSetType == 2)
				return new CompressedNodeSet(nodes, data);
			
			throw new FormatException();
		}

		private static byte[] ReadBinary(XmlReader reader) {
			string base64 = ReadString(reader);
			return Convert.FromBase64String(base64);
		}
		
		private static void WriteLongArray(XmlWriter writer, long[] array) {
			writer.WriteStartElement("longArray");
			for(int i = 0; i < array.Length; i++) {
				writer.WriteElementString("long", Convert.ToString(array[i], CultureInfo.InvariantCulture));
			}
			writer.WriteEndElement();
		}
		
		private static void WriteBinary(XmlWriter writer, byte[] bytes) {
			writer.WriteElementString("binary", Convert.ToBase64String(bytes));
		}

		public void Serialize(MessageStream messageStream, Stream output) {
			StreamWriter streamWriter = new StreamWriter(output, ContentEncoding);
			XmlTextWriter writer = new XmlTextWriter(streamWriter);
			Serialize(messageStream, writer);
			streamWriter.Flush();
		}
		
		public void Serialize(MessageStream messageStream, XmlWriter writer) {
			writer.WriteStartDocument(true);
			writer.WriteStartElement("stream");
			
			foreach(object item in messageStream.Items) {
				if (item == null) {
					writer.WriteStartElement("null");
					writer.WriteFullEndElement();
				} else if (item is string) {
					string s = (string)item;
					if (s.Equals(MessageStream.MessageOpen)) {
						continue;
					} else if (s.Equals(MessageStream.MessageClose)) {
						writer.WriteEndElement();
					} else {
						writer.WriteStartElement("message");
						writer.WriteAttributeString("name", s);
					}
				} else if (item is long) {
					writer.WriteElementString("long", Convert.ToString(item, CultureInfo.InvariantCulture));
					writer.WriteEndElement();
				} else if (item is int) {
				} else if (item is byte[]) {
					writer.WriteElementString("binary", Convert.ToBase64String((byte[])item));
				} else if (item is MessageStream.StringArgument) {
					string s = ((MessageStream.StringArgument)item).Value;
					writer.WriteElementString("string", s);
				} else if (item is long[]) {
				} else if (item is NodeSet) {
					NodeSet nodeSet = (NodeSet)item;
					
					writer.WriteStartElement("nodeSet");
					if (nodeSet is SingleNodeSet) {
						writer.WriteAttributeString("type", "1");
					} else if (nodeSet is CompressedNodeSet) {
						writer.WriteAttributeString("type", "2");
					}
					writer.WriteStartElement("nodes");
					WriteLongArray(writer, nodeSet.NodeIds);
					writer.WriteEndElement();
					writer.WriteStartElement("data");
					WriteBinary(writer, nodeSet.Buffer);
					writer.WriteEndElement();
					writer.WriteEndElement();
				} else if (item is DataAddress) {
				} else if (item is ServiceException) {
				} else if (item is IServiceAddress[]) {
				} else if (item is DataAddress[]) {
				} else if (item is IServiceAddress) {
				} else if (item is String[]) {
				} else if (item is int[]) {
				} else {
					throw new ArgumentException("Unknown message object in list");
				}
			}
			
			writer.WriteEndElement();
			writer.WriteEndDocument();
		}
	}
}