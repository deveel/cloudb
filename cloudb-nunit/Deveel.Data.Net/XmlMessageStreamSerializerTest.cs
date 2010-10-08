using System;
using System.IO;
using System.Text;

using Deveel.Data.Net.Client;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public class XmlRpcMessageSerializerTest {
		private string Serialize(Message messageStream) {
			MemoryStream outputStream = new MemoryStream();
			XmlRpcMessageSerializer messageSerializer = new XmlRpcMessageSerializer();
			messageSerializer.Serialize(messageStream, outputStream);
			outputStream.Position = 0;
			StreamReader reader = new StreamReader(outputStream);
			string line;
			StringBuilder sb = new StringBuilder();
			while((line = reader.ReadLine()) != null) {
				sb.Append(line);
			}
			return sb.ToString();
		}
		
		private Message Deserialize(string s, MessageType messageType) {
			byte[] bytes = Encoding.UTF8.GetBytes(s);
			MemoryStream inputStream = new MemoryStream(bytes);
			XmlRpcMessageSerializer messageSerializer = new XmlRpcMessageSerializer();
			return messageSerializer.Deserialize(inputStream, messageType);
		}
		
		[Test]
		public void Test1_SimpleSerialize() {
			RequestMessage messageStream = new RequestMessage("test");
			messageStream.Arguments.Add("simple");
			
			string result = Serialize(messageStream);
			Console.Out.WriteLine("Result:");
			Console.Out.Write(result);
			Console.Out.Flush();
		}
		
		[Test]
		public void Test2_SimpleDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("<stream>");
			sb.Append("<message name=\"test\">");
			sb.Append("<string>simple</string>");
			sb.Append("</message>");
			sb.Append("</stream>");
			
			Message result = Deserialize(sb.ToString(), MessageType.Response);
		}
	}
}
