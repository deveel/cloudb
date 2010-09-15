using System;
using System.IO;
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public class XmlMessageStreamSerializerTest {
		private string Serialize(MessageStream messageStream) {
			MemoryStream outputStream = new MemoryStream();
			XmlMessageStreamSerializer serializer = new XmlMessageStreamSerializer();
			serializer.Serialize(messageStream, outputStream);
			outputStream.Position = 0;
			StreamReader reader = new StreamReader(outputStream);
			string line;
			StringBuilder sb = new StringBuilder();
			while((line = reader.ReadLine()) != null) {
				sb.Append(line);
			}
			return sb.ToString();
		}
		
		private MessageStream Deserialize(string s) {
			byte[] bytes = Encoding.UTF8.GetBytes(s);
			MemoryStream inputStream = new MemoryStream(bytes);
			XmlMessageStreamSerializer serializer = new XmlMessageStreamSerializer();
			return serializer.Deserialize(inputStream);
		}
		
		[Test]
		public void Test1_SimpleSerialize() {
			MessageStream messageStream = new MessageStream(16);
			messageStream.StartMessage("test");
			messageStream.AddMessageArgument("simple");
			messageStream.CloseMessage();
			
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
			
			MessageStream result = Deserialize(sb.ToString());
		}
	}
}
