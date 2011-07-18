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
			expected.Append("{\"$type\":\"dateTime\",\"format\":\"yyyyMMddTHH:mm:s\",\"value\":\"20090722T11:09:56\"}");
			expected.Append("]");
			expected.Append("}");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(expected.ToString(), s);
		}

		[Test]
		public void SimpleMethodCallDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("{");
			sb.Append("\"jsonrpc\":\"1.0\",");
			sb.Append("\"method\":\"testMethod\",");
			sb.Append("\"params\":[");
			sb.Append("34,");
			sb.Append("{\"$type\":\"dateTime\",\"format\":\"yyyyMMddTHH:mm:s\",\"value\":\"20090722T11:09:56\"}");
			sb.Append("]");
			sb.Append("}");

			RequestMessage request = (RequestMessage)Deserialize(sb.ToString(), MessageType.Request);
			Assert.AreEqual("testMethod", request.Name);
			Assert.AreEqual(34, request.Arguments[0].Value);
			Assert.AreEqual(new DateTime(2009, 07, 22, 11, 09, 56), request.Arguments[1].Value);
		}

		[Test]
		public void ArraySerialize() {
			RequestMessage request = new RequestMessage("testArray");
			request.Arguments.Add(new int[] { 45, 87, 90, 112 });

			string s = Serialize(request);

			StringBuilder sb = new StringBuilder();
			sb.Append("{");
			sb.Append("\"jsonrpc\":\"1.0\",");
			sb.Append("\"method\":\"testArray\",");
			sb.Append("\"params\":[");
			sb.Append("[45,87,90,112]");
			sb.Append("]");
			sb.Append("}");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(sb.ToString(), s);
		}

		[Test]
		public void ArrayDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("{");
			sb.Append("\"jsonrpc\":\"1.0\",");
			sb.Append("\"method\":\"testArray\",");
			sb.Append("\"params\":[");
			sb.Append("[45,87,90,112]");
			sb.Append("]");
			sb.Append("}");

			Message request = Deserialize(sb.ToString(), MessageType.Request);

			Assert.AreEqual("testArray", request.Name);
			Assert.AreEqual(MessageType.Request, request.MessageType);
			Assert.AreEqual(1, request.Arguments.Count);

			object value = request.Arguments[0].Value;
			Assert.IsInstanceOf(typeof(int[]), value);
			int[] array = (int[])value;
			Assert.AreEqual(4, array.Length);
			Assert.AreEqual(45, array[0]);
			Assert.AreEqual(87, array[1]);
			Assert.AreEqual(90, array[2]);
			Assert.AreEqual(112, array[3]);
		}
	}
}