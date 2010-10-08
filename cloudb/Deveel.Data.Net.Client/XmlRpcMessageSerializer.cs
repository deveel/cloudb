using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public sealed class XmlRpcMessageSerializer : XmlMessageSerializer, IMessageStreamSupport {
		private List<IXmlRpcTypeResolver> resolvers = new List<IXmlRpcTypeResolver>();
		
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
				int readCount = 0;
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
			} else if (value is int) {
				WriteInt4(value, format, writer);
			} else if (value is long) {
				WriteInt8(value, format, writer);
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
				WriteArgument(child, xmlWriter);
				xmlWriter.WriteEndElement();
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
			WriteParams(message);
			xmlWriter.WriteEndElement();
		}
		
		private void WriteResponse(Message message, XmlWriter xmlWriter) {
			xmlWriter.WriteStartElement("methodResponse");
			
			if (MessageUtil.HasError(message)) {
				MessageError error = MessageUtil.GetError(message);
				xmlWriter.WriteStartElement("fault");
				WriteValue(error, null, xmlWriter);
				xmlWriter.WriteEndElement();
			} else {
				WriteParams(message);
			}
			
			xmlWriter.WriteEndElement();
		}

		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			throw new NotImplementedException();
		}

		protected override void Serialize(Message message, TextWriter writer) {
			XmlTextWriter xmlWriter = new XmlTextWriter(writer);
			
			bool inStream = message is IMessageStream;
			
			if (inStream) {
				xmlWriter.WriteStartDocument(true);
				IMessageStream stream = (IMessageStream)message;
				xmlWriter.WriteStartElement("messageStream");
				foreach(Message streamedMessage in stream) {
					Serialize(streamedMessage, writer);
				}
				xmlWriter.WriteEndElement();
				xmlWriter.WriteEndDocument();
				return;
			}
			
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
					for(int i = 0; i < nodeSet.NodeIds.Length; i++) {
						serializer.WriteValue(nodeSet.NodeIds[i], null, writer);
					}
					writer.WriteEndElement();
					
					writer.WriteStartElement("data");
					serializer.WriteValue(nodeSet.Buffer, null, writer);
					writer.WriteEndElement();
				
					writer.WriteEndElement();
				}
				
				throw new FormatException();
			}
			
			public object ReadValue(XmlReader reader, Type type)
			{
				throw new NotImplementedException();
			}
		}
		
		#endregion
	}
}