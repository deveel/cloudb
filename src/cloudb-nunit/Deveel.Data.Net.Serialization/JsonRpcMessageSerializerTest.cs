using System;
using System.IO;
using System.Text;

using Deveel.Data.Net.Client;

using NUnit.Framework;

namespace Deveel.Data.Net.Serialization {
	[TestFixture]
	public sealed class JsonRpcMessageSerializerTest {
		private static string Serialize(Message messageStream) {
			MemoryStream outputStream = new MemoryStream();
			JsonRpcMessageSerializer messageSerializer = new JsonRpcMessageSerializer(Encoding.UTF8);
			messageSerializer.Serialize(messageStream, outputStream);
			outputStream.Position = 0;
			StreamReader reader = new StreamReader(outputStream);
			string line;
			StringBuilder sb = new StringBuilder();
			while ((line = reader.ReadLine()) != null) {
				sb.Append(line);
			}
			return sb.ToString();
		}

		private Message Deserialize(string s, MessageType messageType) {
			byte[] bytes = Encoding.UTF8.GetBytes(s);
			MemoryStream inputStream = new MemoryStream(bytes);
			JsonRpcMessageSerializer messageSerializer = new JsonRpcMessageSerializer(Encoding.UTF8);
			return messageSerializer.Deserialize(inputStream, messageType);
		}

		[Test]
		public void SimpleMethodCallSerialize() {
			RequestMessage request = new RequestMessage("testMethod");
			request.Arguments.Add(34);
			request.Arguments.Add(new DateTime(2009, 07, 22, 11, 09, 56));
			string s = Serialize(request);

			StringBuilder expected = new StringBuilder();
			expected.Append("{");
			expected.Append("\"jsonrpc\":\"1.0\",");
			expected.Append("\"method\":\"testMethod\",");
			expected.Append("\"params\":[");
			expected.Append("34,");
			expected.Append("20090722T11:09:56");
			expected.Append("]");
			expected.Append("}");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(expected.ToString(), s);
		}
	}
}