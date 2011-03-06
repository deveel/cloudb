using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net.Serialization {
	public sealed class XmlRestMessageSerializer : XmlMessageSerializer {
		public XmlRestMessageSerializer(string encoding)
			: base(encoding) {
		}

		public XmlRestMessageSerializer(Encoding encoding)
			: base(encoding) {
		}

		public XmlRestMessageSerializer() {
		}

		private static string ReadString(XmlReader reader) {
			if (reader.NodeType != XmlNodeType.Text)
				throw new FormatException("Invalid contents of a string element.");

			return reader.Value;
		}

		private static long ReadLong(XmlReader reader) {
			string text = ReadString(reader);
			long value;
			if (!Int64.TryParse(text, out value))
				throw new FormatException("The content a 'long' element is not valid.");

			return value;
		}

		private static int ReadInt(XmlReader reader) {
			string text = ReadString(reader);
			int value;
			if (!Int32.TryParse(text, out value))
				throw new FormatException("The content of a 'int' element is not valid.");

			return value;
		}

		private static double ReadDouble(XmlReader reader) {
			string text = ReadString(reader);
			double value;
			if (!Double.TryParse(text, out  value))
				throw new FormatException("The content of a 'double' element is not valid.");

			return value;
		}

		private static bool ReadBoolean(XmlReader reader) {
			string text = ReadString(reader);
			if (text == "1")
				return true;
			if (text == "0")
				return false;

			bool value;
			if (!Boolean.TryParse(text, out value))
				throw new FormatException("Invalid format of a 'bool' element.");

			return value;
		}

		private static DateTime ReadDateTime(XmlReader reader, string format) {
			if (reader.NodeType == XmlNodeType.Attribute &&
			    reader.LocalName == "format") {
				format = reader.Value;
				if (!reader.Read())
					throw new FormatException("The dateTime element is not properly formatted.");
			}

			if (reader.NodeType != XmlNodeType.Text)
				throw new FormatException("Invalid contents of a dateTime element.");

			string value = reader.Value;

			if (String.IsNullOrEmpty(value))
				throw new FormatException();

			return !String.IsNullOrEmpty(format)
			       	? DateTime.ParseExact(value, format, CultureInfo.InvariantCulture)
			       	: DateTime.Parse(value);
		}

		private static Stream ReadBinary(XmlReader reader) {
			string text = ReadString(reader);
			byte[] bytes = Convert.FromBase64String(text);
			return new MemoryStream(bytes);
		}

		private static object ReadValue(XmlReader reader, string valueType) {
			if (valueType == "boolean")
				return ReadBoolean(reader);
			if (String.IsNullOrEmpty(valueType) || 
			    valueType == "string")
				return ReadString(reader);
			if (valueType == "int4")
				return ReadInt(reader);
			if (valueType == "int8")
				return ReadLong(reader);
			if (valueType == "double")
				return ReadDouble(reader);
			if (valueType == "binary")
				return ReadBinary(reader);
			if (valueType == "dateTime")
				return ReadDateTime(reader, null);

			// Arrays 
			if (valueType == "stringArray")
				return ReadArray(reader, typeof(string));
			if (valueType == "int4Array")
				return ReadArray(reader, typeof(int));
			if (valueType == "int8Array")
				return ReadArray(reader, typeof(long));
			if (valueType == "doubleArray")
				return ReadArray(reader, typeof(double));
			if (valueType == "dateTimeArray")
				return ReadArray(reader, typeof(DateTime));
			if (valueType == "binaryArray")
				return ReadArray(reader, typeof(Stream));
			if (valueType == "array")
				return ReadArray(reader, null);

			throw new FormatException("Value type '" + valueType + "' not supported.");
		}

		private static Array ReadArray(XmlReader reader, Type type) {
			int elementCount = -1;
			Array array = null;
			int i = -1;

			string format = null;

			while (reader.Read()) {
				XmlNodeType nodeType = reader.NodeType;
				if (nodeType == XmlNodeType.Attribute) {
					if (reader.LocalName == "format") {
						format = reader.Value;
					} else if (reader.LocalName == "length") {
						if (elementCount != -1)
							throw new FormatException();

						string text = reader.Value;
						if (!Int32.TryParse(text, out elementCount))
							throw new FormatException();

						array = Array.CreateInstance(type, elementCount);
					} else {
						throw new FormatException("Invalid attribute.");
					}
				} else if (nodeType == XmlNodeType.Element) {
					if (array == null)
						throw new InvalidOperationException();

					if (type == null) {
						string elemName = reader.LocalName;
						if (elemName == "int4")
							type = typeof(int);
						else if (elemName == "int8")
							type = typeof(long);
						else if (elemName == "string")
							type = typeof(string);
						else if (elemName == "double")
							type = typeof(double);
						else if (elemName == "dateTime")
							type = typeof(DateTime);
						else if (elemName == "boolean")
							type = typeof(bool);
						else if (elemName == "binary")
							type = typeof(Stream);
						else
							throw new FormatException("Unsupported array element name.");
					}

					object value = null;
					if (type == typeof(bool))
						value = ReadBoolean(reader);
					if (type == typeof(int))
						value = ReadInt(reader);
					else if (type == typeof(long))
						value = ReadLong(reader);
					else if (type == typeof(double))
						value = ReadDouble(reader);
					else if (type == typeof(DateTime))
						value = ReadDateTime(reader, format);
					else if (type == typeof(string))
						value = ReadString(reader);
					else if (type == typeof(Stream))
						value = ReadBinary(reader);

					array.SetValue(value, i++);
				}

				if (--elementCount == 0)
					break;
			}

			return array;
		}

		private static MessageArgument ReadArgument(XmlReader reader) {
			MessageArgument argument = null;
			string attributeName = reader.LocalName;

			string valueType = null;
			object value = null;
			while (reader.Read()) {
				XmlNodeType valueNodeType = reader.NodeType;

				if (valueNodeType == XmlNodeType.Comment ||
				    valueNodeType == XmlNodeType.Whitespace)
					continue;

				if (valueNodeType == XmlNodeType.Attribute) {
					if (reader.LocalName == "type") {
						valueType = reader.Value;
						continue;
					}
					if (argument == null)
						argument = new MessageArgument(attributeName, value);

					argument.Attributes.Add(reader.LocalName, reader.Value);
				} else if (reader.NodeType == XmlNodeType.Element) {
					if (argument == null)
						argument = new MessageArgument(attributeName, value);

					argument.Children.Add(ReadArgument(reader));
				} else if (reader.NodeType == XmlNodeType.EndElement) {
					break;
				} else {
					value = ReadValue(reader, valueType);

					if (argument == null) {
						argument = new MessageArgument(attributeName, value);
					} else {
						argument.Value = value;
					}
				}
			}

			return argument;
		}

		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			XmlTextReader xmlReader = new XmlTextReader(reader);
			
			Message message = null;
			
			while (xmlReader.Read()) {
				XmlNodeType nodeType = xmlReader.NodeType;
				//TODO: should we take the 'encoding' attribute?
				if (nodeType == XmlNodeType.XmlDeclaration)
					continue;

				if (nodeType == XmlNodeType.Comment ||
				    nodeType == XmlNodeType.Whitespace)
					continue;

				if (nodeType == XmlNodeType.Element) {
					string elementName = xmlReader.LocalName;
					if (message == null) {
						if (messageType == MessageType.Request)
							message = new RequestMessage(elementName);
						else
							message = new ResponseMessage(elementName);
						
						continue;
					}

					MessageArgument argument = ReadArgument(xmlReader);
					message.Arguments.Add(argument);
				} else if (nodeType == XmlNodeType.Attribute) {
					if (message == null)
						throw new FormatException("Attribute '" + xmlReader.LocalName + "' found at the wrong moment.");
					
					message.Attributes.Add(xmlReader.LocalName, xmlReader.Value);
				} else if (nodeType == XmlNodeType.EndElement) {
					break;
				}
			}
			
			if (message == null)
				throw new FormatException("Invalid format.");
			
			return message;
		}

		protected override void Serialize(Message message, TextWriter writer) {
			XmlTextWriter xmlWriter = new XmlTextWriter(writer);
			
			xmlWriter.WriteStartDocument(true);
			
			if (message.HasName && message.Name == "messageStream")
				throw new FormatException("The root element name 'messageStream' is reserved.");

			string rootElement = message.Name;
			if (message.HasName) {
				if (message.MessageType == MessageType.Request)
					rootElement = "request";
				else
					rootElement = "response";
			}

			xmlWriter.WriteStartElement(rootElement);
			if (message.Attributes.Count > 0) {
				foreach(KeyValuePair<string, object> attribute in message.Attributes) {
					xmlWriter.WriteStartAttribute(attribute.Key);
					xmlWriter.WriteValue(attribute.Value);
					xmlWriter.WriteEndAttribute();
				}
			}

			foreach(MessageArgument argument in message.Arguments) {
				WriteArgument(xmlWriter, argument, true);
			}

			xmlWriter.WriteEndElement();
			xmlWriter.WriteEndDocument();
		}

		private static string GetValueType(Type type) {
			if (type == typeof(string))
				return "string";
			if (type == typeof(bool))
				return "boolean";
			if (type == typeof(int))
				return "int4";
			if (type == typeof(long))
				return "int8";
			if (type == typeof(double))
				return "double";
			if (type == typeof(DateTime))
				return "dateTime";
			if (typeof(Stream).IsAssignableFrom(type))
				return "binary";
			if (type.IsArray) {
				string valueType = GetValueType(type.GetElementType());
				return valueType + "Array";
			}

			throw new FormatException("Invalid value.");
		}

		private static string GetValueType(object value) {
			if (value == null)
				return "null";

			Type type = value.GetType();
			return GetValueType(type);
		}

		private static void WriteArgument(XmlWriter writer, MessageArgument argument, bool printType) {
			writer.WriteStartElement(argument.Name);

			if (argument.Attributes.Count > 0) {
				foreach(KeyValuePair<string, object> attribute in argument.Attributes) {
					writer.WriteStartAttribute(attribute.Key);
					if (attribute.Value != null)
						writer.WriteValue(attribute.Value);
					writer.WriteEndAttribute();
				}
			}

			object value = argument.Value;

			if (value != null) {
				string valueType = GetValueType(value);

				if (printType)
					writer.WriteAttributeString("type", valueType);

				if (value is Array) {
					string elemType = valueType.Substring(0, valueType.Length - 5);

					Array array = (Array) value;
					int length = array.GetLength(0);
					for (int i = 0; i < length; i++) {
						// this is an ugly hack, but speeds work ...
						WriteArgument(writer, new MessageArgument(elemType, array.GetValue(i)), false);
					}
				} else {
					if (value is DateTime) {
						string format = argument.Format;
						value = !String.IsNullOrEmpty(format) ? ((DateTime) value).ToString(format) : ((DateTime) value).ToString();
					} else if (value is Stream) {
						Stream stream = (Stream) value;
						byte[] bytes = new byte[stream.Length];
						stream.Read(bytes, 0, bytes.Length);
						value = Convert.ToBase64String(bytes);
					}

					writer.WriteValue(value);
				}
			}

			writer.WriteEndElement();
		}
	}
}