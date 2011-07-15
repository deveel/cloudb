using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net.Serialization {
	public sealed class XmlRpcMessageSerializer : XmlMessageSerializer, IRpcMessageSerializer {
		private readonly List<IXmlRpcTypeResolver> resolvers = new List<IXmlRpcTypeResolver>();
		
		public XmlRpcMessageSerializer(string encoding)
			: base(encoding) {
			resolvers.Add(new IRpcTypeResolver(this));
		}

		public XmlRpcMessageSerializer(Encoding encodig)
			: base(encodig) {
			resolvers.Add(new IRpcTypeResolver(this));
		}

		public XmlRpcMessageSerializer() {
			resolvers.Add(new IRpcTypeResolver(this));
		}
		
		private const string DateTimeIso8601Format = "yyyyMMddTHH:mm:s";
		
		public IXmlRpcTypeResolver TypeResolver {
			get { return resolvers.Count == 2 ? resolvers[1] : null; }
			set {
				resolvers.Clear();
				resolvers.Add(new IRpcTypeResolver(this));
				if (value != null)
					resolvers.Add(value);
			}
		}

		public bool SupportsMessageStream {
			get { return true; }
		}
		
		public void AddTypeResolver(IXmlRpcTypeResolver resolver) {
			if (resolvers == null)
				throw new ArgumentNullException("resolver");
			
			for(int i = 0; i < resolvers.Count; i++) {
				if (resolvers[i].GetType() == resolver.GetType())
					throw new ArgumentException("Another resolver of type '" + resolver.GetType() +  "' was already present.");
			}
			
			resolvers.Add(resolver);
		}
		
		private Type ResolveType(string elementName, out IXmlRpcTypeResolver resolver) {
			for(int i = 0; i < resolvers.Count; i++) {
				Type type;
				resolver = resolvers[i];
				if ((type = resolver.ResolveType(elementName)) != null) {
					return type;
				}
			}
			
			resolver = null;
			return null;
		}
		
		private string ResolveElementName(object value, out IXmlRpcTypeResolver resolver) {
			for(int i = 0; i < resolvers.Count; i++) {
				string elementName;
				resolver = resolvers[i];
				if (!String.IsNullOrEmpty(elementName = resolver.ResolveElementName(value.GetType()))) {
					return elementName;
				}
			}
			
			resolver = null;
			return null;
		}
		
		private string Format(object value, string format) {
			if (value is IFormattable && !String.IsNullOrEmpty(format))
				return ((IFormattable)value).ToString(format, CultureInfo.InvariantCulture);
			return value.ToString();
		}
		
		private void WriteNil(XmlWriter writer) {
			writer.WriteStartElement("nil");
			writer.WriteFullEndElement();
		}
		
		private void WriteBoolean(object value, XmlWriter writer) {
			writer.WriteStartElement("boolean");
			bool b = (bool)value;
			writer.WriteValue(b ? 1 : 0);
			writer.WriteEndElement();
		}

		public void WriteInt1(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("i1");
			writer.WriteString(Format(value, format));
			writer.WriteEndElement();
		}

		private void WriteInt2(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("i2");
			writer.WriteValue(Format(value, format));
			writer.WriteEndElement();
		}
		
		private void WriteInt4(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("i4");
			writer.WriteValue(Format(value, format));
			writer.WriteEndElement();
		}
		
		private void WriteInt8(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("i8");
			writer.WriteValue(Format(value, format));
			writer.WriteEndElement();
		}

		private void WriteFloat(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("float");
			writer.WriteString(Format(value, format));
			writer.WriteEndElement();
		}
		
		private void WriteDouble(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("double");
			writer.WriteValue(Format(value, format));
			writer.WriteEndElement();
		}
		
		private void WriteDateTime(object value, string format, XmlWriter writer) {
			if (String.IsNullOrEmpty(format)) {
				writer.WriteStartElement("dateTime.iso8601");
				writer.WriteValue(Format(value, DateTimeIso8601Format));
				writer.WriteEndElement();
			} else {
				writer.WriteStartElement("dateTime");
				writer.WriteValue(Format(value, format));
				writer.WriteEndElement();
			}
		}
		
		private void WriteString(object value, XmlWriter writer) {
			writer.WriteStartElement("string");
			writer.WriteValue(value);
			writer.WriteEndElement();
		}
		
		private void WriteBinary(object value, XmlWriter writer) {
			writer.WriteStartElement("base64");
				
			byte[] bytes;
			if (value is byte[]) {
				bytes =  (byte[])((byte[])value).Clone();
			} else {
				Stream inputStream = (Stream)value;
				MemoryStream memoryStream = new MemoryStream();
				int readCount;
				byte[] temp = new byte[1024];
				while((readCount = inputStream.Read(temp, 0, temp.Length)) > 0) {
					memoryStream.Write(temp, 0, readCount);
				}
				
				memoryStream.Flush();
				bytes = memoryStream.ToArray();
			}
			
			string s = Convert.ToBase64String(bytes);
			writer.WriteValue(s);
			writer.WriteEndElement();
		}
		
		private void WriteArray(object value, string format, XmlWriter writer) {
			Array array = (Array)value;
			writer.WriteStartElement("array");
			writer.WriteStartElement("data");
				
			for(int i = 0; i < array.Length; i++) {
				WriteValue(array.GetValue(i), format, writer);
			}
			
			writer.WriteEndElement();
			writer.WriteEndElement();
		}
		
		private void WriteMessageError(object value, XmlWriter writer) {
			MessageError error = (MessageError)value;
			writer.WriteStartElement("struct");
			
			// error message 
			writer.WriteStartElement("member");
			writer.WriteStartElement("name");
			writer.WriteValue("message");
			writer.WriteEndElement();
			WriteValue(error.Message, null, writer);
			writer.WriteEndElement();
			
			if (!String.IsNullOrEmpty(error.StackTrace)) {
				writer.WriteStartElement("member");
				writer.WriteStartElement("name");
				writer.WriteValue("stackTrace");
				writer.WriteEndElement();
				WriteValue(error.StackTrace, null, writer);
				writer.WriteEndElement();
			}
			
			if (!String.IsNullOrEmpty(error.Source)) {
				writer.WriteStartElement("member");
				writer.WriteStartElement("name");
				writer.WriteValue("source");
				writer.WriteEndElement();
				WriteValue(error.Source, null, writer);
				writer.WriteEndElement();
			}
			
			writer.WriteEndElement();
		}
		
		private void WriteValue(object value, string format, XmlWriter writer) {
			writer.WriteStartElement("value");
			
			if (value == null || value == DBNull.Value) {
				WriteNil(writer);
			} else  if (value is bool) {
				WriteBoolean(value, writer);
			} else if (value is byte) {
				WriteInt1(value, format, writer);
			} else if (value is short) {
				WriteInt2(value, format, writer);
			} else if (value is int) {
				WriteInt4(value, format, writer);
			} else if (value is long) {
				WriteInt8(value, format, writer);
			} else if (value is float) {
				WriteFloat(value, format, writer);
			} else if (value is double) {
				WriteDouble(value, format, writer);
			} else if (value is DateTime) {
				WriteDateTime(value, format, writer);
			} else if (value is string) {
				WriteString(value, writer);
			} else if (value is byte[] || 
			           value is Stream) {
				WriteBinary(value, writer);
			} else if (value is Array) {
				WriteArray(value, format, writer);
			} else if (value is MessageError) {
				WriteMessageError(value, writer);
			} else {
				string resolvedElementName;
				IXmlRpcTypeResolver resolver = null;
				
				try {
					resolvedElementName = ResolveElementName(value, out resolver);
				} catch (Exception) {
					resolvedElementName = null;
				}
				
				if (resolvedElementName == null)
					throw new FormatException("Unable to resolve value");
				
				resolver.WriteValue(writer, resolvedElementName, value, format);
			}
			
			writer.WriteEndElement();
		}
		
		private void WriteComplexArgument(MessageArgument argument, XmlWriter xmlWriter) {
			xmlWriter.WriteStartElement("value");
			xmlWriter.WriteStartElement("struct");
			
			for(int i = 0; i < argument.Children.Count; i++) {
				MessageArgument child = argument.Children[i];
				
				xmlWriter.WriteStartElement("member");
				xmlWriter.WriteStartElement("name");
				xmlWriter.WriteValue(child.Name);
				xmlWriter.WriteEndElement();
				WriteArgument(child, xmlWriter);
				xmlWriter.WriteEndElement();
			}
			
			xmlWriter.WriteEndElement();
			xmlWriter.WriteEndElement();
		}
		
		private void WriteArgument(MessageArgument argument, XmlWriter xmlWriter) {
			if (argument.Children.Count == 0) {
				WriteValue(argument.Value, argument.Format, xmlWriter);
			} else {
				WriteComplexArgument(argument, xmlWriter);
			}
		}
		
		private void WriteParams(Message message, XmlWriter xmlWriter) {
			if (message.Arguments.Count > 0) {
				xmlWriter.WriteStartElement("params");
				
				for (int i = 0; i < message.Arguments.Count; i++) {
					MessageArgument arg = message.Arguments[i];
					xmlWriter.WriteStartElement("param");
					WriteArgument(arg, xmlWriter);
					xmlWriter.WriteEndElement();
				}
				
				xmlWriter.WriteEndElement();
			}
		}
		
		private void WriteRequest(Message message, XmlWriter xmlWriter) {
			xmlWriter.WriteStartElement("methodCall");
			
			// methodName
			xmlWriter.WriteStartElement("methodName");
			xmlWriter.WriteValue(message.Name);
			xmlWriter.WriteEndElement();
			
			// params
			WriteParams(message, xmlWriter);
			xmlWriter.WriteEndElement();
		}
		
		private void WriteResponse(Message message, XmlWriter xmlWriter) {
			xmlWriter.WriteStartElement("methodResponse");
			
			if (message.HasError) {
				MessageError error = message.Error;
				xmlWriter.WriteStartElement("fault");
				WriteValue(error, null, xmlWriter);
				xmlWriter.WriteEndElement();
			} else {
				WriteParams(message, xmlWriter);
			}
			
			xmlWriter.WriteEndElement();
		}

		private void ReadParams(Message message, XmlReader xmlReader) {
			while (xmlReader.Read()) {
				if (xmlReader.NodeType == XmlNodeType.Comment)
					continue;

				if (xmlReader.NodeType == XmlNodeType.EndElement) {
					if (xmlReader.LocalName == "params")
						break;
					if (xmlReader.LocalName != "param")
						throw new FormatException();
					continue;
				}

				if (xmlReader.NodeType != XmlNodeType.Element)
					throw new FormatException();
				if (xmlReader.LocalName != "param")
					throw new FormatException();

				object value = ReadValue(xmlReader);
				if (value is IDictionary<string, object>) {
					MessageArgument argument = new MessageArgument();
					AddValues(argument, (IDictionary<string,object>)value);
					message.Arguments.Add(argument);
				} else {
					message.Arguments.Add(value);
				}
			}
		}

		private static void AddValues(MessageArgument argument, IDictionary<string,object> values) {
			foreach (KeyValuePair<string, object> pair in values) {
				object value = pair.Value;
				if (value is IDictionary<string, object>) {
					MessageArgument child = new MessageArgument(pair.Key);
					AddValues(argument, (IDictionary<string, object>)value);
					argument.Children.Add(child);
				} else {
					argument.Children.Add(pair.Key, value);
				}
			}
		}

		private static bool ReadBoolean(XmlReader xmlReader) {
			string value = ReadString(xmlReader);
			if (value == "1" ||
			    String.Compare(value, "true", true) == 0)
				return true;
			if (value == "0" ||
			    String.Compare(value, "false", true) == 0)
				return false;

			throw new FormatException("Invalid boolean value (" + value + ")");
		}

		private static byte ReadInt1(XmlReader xmlReader) {
			string s = ReadString(xmlReader);
			byte value;
			if (!Byte.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid i1.");
			return value;
		}

		private static short ReadInt2(XmlReader xmlReader) {
			string s = ReadString(xmlReader);
			short value;
			if (!Int16.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid i2.");
			return value;
		}

		private static int ReadInt4(XmlReader xmlReader) {
			string s = ReadString(xmlReader);
			int value;
			if (!Int32.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid i4.");
			return value;
		}

		private static long ReadInt8(XmlReader xmlReader) {
			string s = ReadString(xmlReader);
			long value;
			if (!Int64.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid i8.");
			return value;
		}

		private static float ReadFloat(XmlReader xmlReader) {
			string s = ReadString(xmlReader);
			float value;
			if (!Single.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid float.");
			return value;
		}

		private static double ReadDouble(XmlReader xmlReader) {
			string s = ReadString(xmlReader);
			double value;
			if (!Double.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid double.");
			return value;
		}

		private static DateTime ReadDateTime(XmlReader xmlReader) {
			string format = null;
			if (xmlReader.LocalName == "dateTime.iso8601")
				format = DateTimeIso8601Format;

			string s = ReadString(xmlReader);
			DateTime value;
			if (format == null && !DateTime.TryParse(s, out value))
				throw new FormatException("The value '" + s + "' is not a valid dateTime.");
			if (!DateTime.TryParseExact(s, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out value))
				throw new FormatException("The value '" + s + "' is not a valid dateTime (ISO 8601).");
			return value;
		}

		private static string ReadString(XmlReader xmlReader) {
			if (!xmlReader.Read())
				throw new FormatException();
			if (xmlReader.NodeType != XmlNodeType.Text)
				throw new FormatException("Invalid content.");
			return xmlReader.Value;
		}

		private IDictionary<string, object> ReadStruct(XmlReader xmlReader) {
			Dictionary<string, object> messageStruct = new Dictionary<string, object>();

			while (xmlReader.Read()) {
				if (xmlReader.NodeType == XmlNodeType.EndElement) {
					if (xmlReader.LocalName == "struct")
						break;

					if (xmlReader.LocalName != "member")
						throw new FormatException();

					continue;
				}

				if (xmlReader.NodeType != XmlNodeType.Element)
					throw new FormatException();
				if (xmlReader.LocalName != "member")
					throw new FormatException();

				if (!xmlReader.Read())
					throw new FormatException();
				if (xmlReader.LocalName != "name")
					throw new FormatException();
				if (!xmlReader.Read())
					throw new FormatException();
				if (xmlReader.NodeType != XmlNodeType.Text)
					throw new FormatException();

				string memberName = xmlReader.Value;

				if (!xmlReader.Read())
					throw new FormatException();
				if (xmlReader.NodeType != XmlNodeType.EndElement)
					throw new FormatException();
				if (xmlReader.LocalName != "name")
					throw new FormatException();

				object value = ReadValue(xmlReader);

				messageStruct.Add(memberName, value);
			}

			return messageStruct;
		}

		private Array ReadArray(XmlReader xmlReader) {
			if (!xmlReader.Read())
				throw new FormatException();
			if (xmlReader.LocalName != "data")
				throw new FormatException();

			List<object> values = new List<object>();
			Type elementType = null;
			bool hasElementType = true;

			while (xmlReader.Read()) {
				if (xmlReader.NodeType == XmlNodeType.EndElement) {
					if (xmlReader.LocalName == "data")
						break;
					if (xmlReader.LocalName == "value")
						continue;
				}
				if (xmlReader.NodeType != XmlNodeType.Element)
					throw new FormatException();
				if (xmlReader.LocalName != "value")
					throw new FormatException();

				object value = ReadConsumedValue(xmlReader);

				if (value != null) {
					Type valueType = value.GetType();
					if (elementType == null && hasElementType) {
						elementType = valueType;
					} else if (elementType != valueType) {
						hasElementType = false;
						elementType = null;
					}
				}

				values.Add(value);
			}

			if (!hasElementType)
				return values.ToArray();

			if (elementType == null)
				throw new FormatException();

			Array array = Array.CreateInstance(elementType, values.Count);
			for (int i = 0; i < values.Count; i++) {
				array.SetValue(values[i], i);
			}
			return array;
		}

		private static Stream ReadBase64(XmlReader xmlReader) {
			string s = ReadString(xmlReader);

			byte[] bytes;
			try {
				bytes = Convert.FromBase64String(s);
			} catch(Exception) {
				throw new FormatException("Invalid base-64 value specified.");
			}

			return new MemoryStream(bytes);
		}

		private object ReadConsumedValue(XmlReader xmlReader) {
			if (!xmlReader.Read())
				throw new FormatException();

			if (xmlReader.NodeType != XmlNodeType.Element)
				throw new FormatException();

			string elementName = xmlReader.LocalName;
			object value;
			bool checkLast = true;

			if (elementName == "nil") {
				value = null;
			} else if (elementName == "boolean") {
				value = ReadBoolean(xmlReader);
			} else if (elementName == "i1" ||
			           elementName == "byte") {
				value = ReadInt1(xmlReader);
			} else if (elementName == "i2" ||
			           elementName == "short") {
				value = ReadInt2(xmlReader);
			} else if (elementName == "i4" ||
			           elementName == "int") {
				value = ReadInt4(xmlReader);
			} else if (elementName == "i8" ||
			           elementName == "long") {
				value = ReadInt8(xmlReader);
			} else if (elementName == "float") {
				value = ReadFloat(xmlReader);
			} else if (elementName == "double") {
				value = ReadDouble(xmlReader);
			} else if (elementName == "dateTime" ||
			           elementName == "dateTime.iso8601") {
				value = ReadDateTime(xmlReader);
			} else if (elementName == "string") {
				value = ReadString(xmlReader);
			} else if (elementName == "base64") {
				value = ReadBase64(xmlReader);
			} else if (elementName == "struct") {
				value = ReadStruct(xmlReader);
				checkLast = false;
			} else if (elementName == "array") {
				value = ReadArray(xmlReader);
			} else {
				IXmlRpcTypeResolver resolver;
				Type type;

				try {
					type = ResolveType(elementName, out resolver);
				} catch(Exception) {
					throw new FormatException("Unable to resolve the element '" + elementName + "' within this context.");
				}

				try {
					value = resolver.ReadValue(xmlReader, type);
				} catch(Exception e) {
					throw new FormatException("Unable to read value for element '" + elementName + "': " + e.Message, e);
				}
			}

			if (checkLast) {
				if (!xmlReader.Read())
					throw new FormatException();
				if (xmlReader.NodeType != XmlNodeType.EndElement)
					throw new FormatException();
				if (xmlReader.LocalName != elementName)
					throw new FormatException();
			}

			return value;
		}

		private object ReadValue(XmlReader xmlReader) {
			if (!xmlReader.Read())
				throw new FormatException();
			if (xmlReader.LocalName != "value")
				throw new FormatException();

			object value = ReadConsumedValue(xmlReader);

			if (!xmlReader.Read())
				throw new FormatException();
			if (xmlReader.LocalName != "value")
				throw new FormatException();

			return value;
		}

		private Message Deserialize(XmlReader xmlReader, MessageType messageType) {
			Message message = null;

			while (xmlReader.Read()) {
				XmlNodeType nodeType = xmlReader.NodeType;

				if (nodeType == XmlNodeType.DocumentType ||
				    nodeType == XmlNodeType.Document ||
				    nodeType == XmlNodeType.Comment ||
				    nodeType == XmlNodeType.XmlDeclaration)
					continue;

				if (nodeType == XmlNodeType.Element) {
					string elementName = xmlReader.LocalName;
					if (message == null) {
						if (elementName == "methodCall") {
							if (!xmlReader.Read())
								throw new FormatException();
							if (xmlReader.NodeType != XmlNodeType.Element)
								throw new FormatException("Invalid node type found.");

							if (xmlReader.LocalName != "methodName")
								throw new FormatException("Unexpected element name.");
							if (!xmlReader.Read())
								throw new FormatException("Method name not found.");
							if (xmlReader.NodeType != XmlNodeType.Text)
								throw new FormatException("Invalid content in method name element.");

							message = new RequestMessage(xmlReader.Value);
						} else if (elementName == "methodResponse") {
							message = new ResponseMessage();
						} else if (elementName == "messageStream") {
							message = new MessageStream(messageType);

							while (xmlReader.Read()) {
								if (xmlReader.NodeType == XmlNodeType.EndElement) {
									if (xmlReader.LocalName == "message")
										continue;
									if (xmlReader.LocalName == "messageStream")
										break;
								}
								
								((MessageStream)message).AddMessage(Deserialize(xmlReader, messageType));
							}
						} else {
							throw new FormatException("Invalid root element name.");
						}
					} else if (xmlReader.LocalName == "fault") {
						if (messageType != MessageType.Response)
							throw new FormatException("Fault element found in a request message.");

						object value = ReadValue(xmlReader);
						if (!(value is MessageError))
							throw new FormatException();
					} else if (xmlReader.LocalName == "params") {
						ReadParams(message, xmlReader);
					} else {
						throw new FormatException("Invalid element name.");
					}
				} else if (nodeType == XmlNodeType.EndElement) {
					string elementName = xmlReader.LocalName;
					if (elementName == "methodCall" ||
					    elementName == "methodResponse")
						break;
					continue;
				} else {
					throw new FormatException("Invalid node type.");
				}
			}

			if (message == null)
				throw new FormatException("Invalid format");

			if (message.MessageType != messageType)
				throw new FormatException("The returned message is not expected.");

			return message;
		}

		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			XmlTextReader xmlReader = new XmlTextReader(reader);
			return Deserialize(xmlReader, messageType);
		}

		private void Serialize(Message message, XmlWriter xmlWriter, bool inStream) {
			if (!inStream)
				xmlWriter.WriteStartDocument(true);

			if (message.MessageType == MessageType.Request) {
				WriteRequest(message, xmlWriter);
			} else {
				WriteResponse(message, xmlWriter);
			}

			if (!inStream)
				xmlWriter.WriteEndDocument();
		}

		protected override void Serialize(Message message, TextWriter writer) {
			XmlTextWriter xmlWriter = new XmlTextWriter(writer);

			if (message is MessageStream) {
				xmlWriter.WriteStartDocument(true);
				MessageStream stream = (MessageStream)message;
				xmlWriter.WriteStartElement("messageStream");
				foreach (Message streamedMessage in stream) {
					xmlWriter.WriteStartElement("message");
					Serialize(streamedMessage, xmlWriter, true);
					xmlWriter.WriteEndElement();
				}
				xmlWriter.WriteEndElement();
				xmlWriter.WriteEndDocument();
			} else {
				Serialize(message, xmlWriter, false);
			}
		}
		
		#region IRpcTypeResolver
		
		class IRpcTypeResolver : IXmlRpcTypeResolver {
			private readonly XmlRpcMessageSerializer serializer;
			
			public IRpcTypeResolver(XmlRpcMessageSerializer serializer) {
				this.serializer = serializer;
			}
			
			public Type ResolveType(string elementName) {
				if (elementName == "dataAddress")
					return typeof(DataAddress);
				if (elementName == "singleNodeSet")
					return typeof(SingleNodeSet);
				if (elementName == "compressedNodeSet")
					return typeof(CompressedNodeSet);
				if (elementName == "serviceAddress")
					return typeof(IServiceAddress);
				
				return null;
			}
						
			public string ResolveElementName(Type type) {
				if (type == typeof(DataAddress))
					return "dataAddress";
				if (type == typeof(SingleNodeSet))
					return "singleNodeSet";
				if (type == typeof(CompressedNodeSet))
					return "compressedNodeSet";
				if (typeof(IServiceAddress).IsAssignableFrom(type))
					return "serviceAddress";
				
				return null;
			}
			
			public void WriteValue(XmlWriter writer, string elementName, object value, string format) {
				if (elementName == "dataAddress") {
					DataAddress address = (DataAddress)value;
					writer.WriteStartElement("dataAddress");
					writer.WriteStartElement("blockId");
					serializer.WriteValue(address.BlockId, null, writer);
					writer.WriteEndElement();
				
					writer.WriteStartElement("dataId");
					serializer.WriteValue(address.DataId, null, writer);
					writer.WriteEndElement();
				
					writer.WriteEndElement();
				} else if (elementName == "serviceAddress") {
					IServiceAddress address = (IServiceAddress)value;
					serializer.WriteValue(address.ToString(), null, writer);
				} else if (elementName == "singleNodeSet" ||
						   elementName == "compressedNodeSet") {
					NodeSet nodeSet = (NodeSet)value;
					if (nodeSet is SingleNodeSet) {
						writer.WriteStartElement("singleNodeSet");
					} else if (nodeSet is CompressedNodeSet) {
						writer.WriteStartElement("compressedNodeSet");
					} else {
						throw new FormatException("NodeSet type not yet supported");
					}

					writer.WriteStartElement("nids");
					for (int i = 0; i < nodeSet.NodeIds.Length; i++) {
						serializer.WriteValue(nodeSet.NodeIds[i], null, writer);
					}
					writer.WriteEndElement();

					writer.WriteStartElement("data");
					serializer.WriteValue(nodeSet.Buffer, null, writer);
					writer.WriteEndElement();

					writer.WriteEndElement();
				} else {
					throw new FormatException();
				}
			}
			
			public object ReadValue(XmlReader reader, Type type) {
				throw new NotImplementedException();
			}
		}
		
		#endregion
	}
}