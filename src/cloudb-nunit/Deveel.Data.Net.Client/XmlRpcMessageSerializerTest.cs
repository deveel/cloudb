using System;
using System.IO;
using System.Text;

using Deveel.Data.Net.Serialization;

using NUnit.Framework;

namespace Deveel.Data.Net.Client {
	[TestFixture]
	public class XmlRpcMessageSerializerTest {
		private static string Serialize(Message messageStream) {
			MemoryStream outputStream = new MemoryStream();
			XmlRpcMessageSerializer messageSerializer = new XmlRpcMessageSerializer(Encoding.UTF8);
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
			XmlRpcMessageSerializer messageSerializer = new XmlRpcMessageSerializer(Encoding.UTF8);
			return messageSerializer.Deserialize(inputStream, messageType);
		}
		
		[Test]
		public void SimpleMethodCallSerialize() {
			RequestMessage request = new RequestMessage("testMethod");
			request.Arguments.Add(34);
			request.Arguments.Add(new DateTime(2009, 07, 22, 11, 09, 56));
			string s = Serialize(request);

			StringBuilder expected = new StringBuilder();
			expected.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			expected.Append("<methodCall>");
			expected.Append("<methodName>testMethod</methodName>");
			expected.Append("<params>");
			expected.Append("<param><value><i4>34</i4></value></param>");
			expected.Append("<param><value><dateTime.iso8601>20090722T11:09:56</dateTime.iso8601></value></param>");
			expected.Append("</params>");
			expected.Append("</methodCall>");
			
			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(expected.ToString(), s);
		}

		[Test]
		public void SimpleMethodCallDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMethod</methodName>");
			sb.Append("<params>");
			sb.Append("<param><value><i4>34</i4></value></param>");
			sb.Append("<param><value><dateTime.iso8601>20090722T11:09:56</dateTime.iso8601></value></param>");
			sb.Append("</params>");
			sb.Append("</methodCall>");

			Message message = Deserialize(sb.ToString(), MessageType.Request);
			Assert.AreEqual(MessageType.Request, message.MessageType);
			Assert.AreEqual("testMethod", message.Name);
			Assert.AreEqual(34, message.Arguments[0].Value);
			Assert.IsTrue(new DateTime(2009, 07, 22, 11, 09, 56).Equals(message.Arguments[1].Value));
		}

		[Test]
		public void MethodCallWithStructSerialize() {
			RequestMessage request = new RequestMessage("testMethod");
			request.Arguments.Add(34);
			request.Arguments.Add(new DateTime(2009, 07, 22, 11, 09, 56));
			MessageArgument arg = new MessageArgument();
			arg.Children.Add("testParam1", "test");
			arg.Children.Add("testParam2", 67.900).Format = "0.000";
			request.Arguments.Add(arg);
			string s = Serialize(request);

			StringBuilder expected = new StringBuilder();
			expected.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			expected.Append("<methodCall>");
			expected.Append("<methodName>testMethod</methodName>");
			expected.Append("<params>");
			expected.Append("<param><value><i4>34</i4></value></param>");
			expected.Append("<param><value><dateTime.iso8601>20090722T11:09:56</dateTime.iso8601></value></param>");
			expected.Append("<param><value>");
			expected.Append("<struct>");
			expected.Append("<member><name>testParam1</name><value><string>test</string></value></member>");
			expected.Append("<member><name>testParam2</name><value><double>67.900</double></value></member>");
			expected.Append("</struct>");
			expected.Append("</value></param>");
			expected.Append("</params>");
			expected.Append("</methodCall>");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(expected.ToString(), s);
		}

		[Test]
		[Category("KnownUnstable")]
		public void MethodCallWithStructDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMethod</methodName>");
			sb.Append("<params>");
			sb.Append("<param><value><i4>34</i4></value></param>");
			sb.Append("<param><value><dateTime.iso8601>20090722T11:09:56</dateTime.iso8601></value></param>");
			sb.Append("<param><value>");
			sb.Append("<struct>");
			sb.Append("<member><name>testParam1</name><value><string>test</string></value></member>");
			sb.Append("<member><name>testParam2</name><value><double>67.900</double></value></member>");
			sb.Append("</struct>");
			sb.Append("</value></param>");
			sb.Append("</params>");
			sb.Append("</methodCall>");

			Message message = Deserialize(sb.ToString(), MessageType.Request);
			Assert.AreEqual(MessageType.Request, message.MessageType);
			Assert.AreEqual("testMethod", message.Name);
			Assert.AreEqual(34, message.Arguments[0].Value);
			Assert.IsTrue(new DateTime(2009, 07, 22, 11, 09, 56).Equals(message.Arguments[1].Value));
			Assert.AreEqual(2, message.Arguments[2].Children.Count);
			Assert.AreEqual("test", message.Arguments[2].Children[0].Value);
			Assert.AreEqual(67.900d, (double)message.Arguments[2].Children[1].Value);
		}

		[Test]
		public void ErrorResponseSerialize() {
			ResponseMessage response = new ResponseMessage();
			response.Arguments.Add(new MessageError(new Exception("Test message for error serialization.")));

			string s = Serialize(response);

			StringBuilder expected = new StringBuilder();
			expected.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			expected.Append("<methodResponse>");
			expected.Append("<fault>");
			expected.Append("<value><struct><member>name</member><value>Test message for error serialization.</value></struct></value>");
			expected.Append("</fault>");
			expected.Append("</methodCall>");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);
		}

		[Test]
		public void ArraySerialize() {
			RequestMessage request = new RequestMessage("testArray");
			request.Arguments.Add(new int[] {45, 87, 90, 112});

			string s = Serialize(request);

			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testArray</methodName>");
			sb.Append("<params>");
			sb.Append("<param><value>");
			sb.Append("<array><data>");
			sb.Append("<value><i4>45</i4></value>");
			sb.Append("<value><i4>87</i4></value>");
			sb.Append("<value><i4>90</i4></value>");
			sb.Append("<value><i4>112</i4></value>");
			sb.Append(("</data></array>"));
			sb.Append("</value></param>");
			sb.Append("</params>");
			sb.Append("</methodCall>");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(sb.ToString(), s);

			// mixed array

			request = new RequestMessage("testMixedArray");
			request.Arguments.Add(new object[] {22, "test1", 56l, true});

			s = Serialize(request);

			sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMixedArray</methodName>");
			sb.Append("<params>");
			sb.Append("<param><value>");
			sb.Append("<array><data>");
			sb.Append("<value><i4>22</i4></value>");
			sb.Append("<value><string>test1</string></value>");
			sb.Append("<value><i8>56</i8></value>");
			sb.Append("<value><boolean>1</boolean></value>");
			sb.Append(("</data></array>"));
			sb.Append("</value></param>");
			sb.Append("</params>");
			sb.Append("</methodCall>");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(sb.ToString(), s);
		}

		[Test]
		public void ArrayDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testArray</methodName>");
			sb.Append("<params>");
			sb.Append("<param><value>");
			sb.Append("<array><data>");
			sb.Append("<value><i4>45</i4></value>");
			sb.Append("<value><i4>87</i4></value>");
			sb.Append("<value><i4>90</i4></value>");
			sb.Append("<value><i4>112</i4></value>");
			sb.Append(("</data></array>"));
			sb.Append("</value></param>");
			sb.Append("</params>");
			sb.Append("</methodCall>");

			Message request = Deserialize(sb.ToString(), MessageType.Request);

			Assert.AreEqual("testArray", request.Name);
			Assert.AreEqual(MessageType.Request, request.MessageType);
			Assert.AreEqual(1, request.Arguments.Count);

			object value = request.Arguments[0].Value;
			Assert.IsInstanceOf(typeof(int[]), value);
			int [] array = (int[]) value;
			Assert.AreEqual(4, array.Length);
			Assert.AreEqual(45, array[0]);
			Assert.AreEqual(87, array[1]);
			Assert.AreEqual(90, array[2]);
			Assert.AreEqual(112, array[3]);

			sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMixedArray</methodName>");
			sb.Append("<params>");
			sb.Append("<param><value>");
			sb.Append("<array><data>");
			sb.Append("<value><i4>22</i4></value>");
			sb.Append("<value><string>test1</string></value>");
			sb.Append("<value><i8>56</i8></value>");
			sb.Append("<value><boolean>1</boolean></value>");
			sb.Append(("</data></array>"));
			sb.Append("</value></param>");
			sb.Append("</params>");
			sb.Append("</methodCall>");

			request = Deserialize(sb.ToString(), MessageType.Request);

			Assert.AreEqual("testMixedArray", request.Name);
			Assert.AreEqual(MessageType.Request, request.MessageType);
			Assert.AreEqual(1, request.Arguments.Count);

			value = request.Arguments[0].Value;
			Assert.IsInstanceOf(typeof(object[]), value);
			object [] mixedArray = (object[])value;
			Assert.AreEqual(4, mixedArray.Length);
			Assert.AreEqual(22, mixedArray[0]);
			Assert.AreEqual("test1", mixedArray[1]);
			Assert.AreEqual(56l, mixedArray[2]);
			Assert.AreEqual(true, mixedArray[3]);
		}

		[Test]
		public void RequestMessageStreamSerialize() {
			MessageStream requestStream = MessageStream.NewRequest();
			RequestMessage request = new RequestMessage("testMethod1");
			request.Arguments.Add(true);
			requestStream.AddMessage(request);
			request = new RequestMessage("testMethod2");
			request.Arguments.Add("test");
			request.Arguments.Add(false);
			requestStream.AddMessage(request);

			string s = Serialize(requestStream);

			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<messageStream>");
			sb.Append("<message>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMethod1</methodName>");
			sb.Append("<params><param><value><boolean>1</boolean></value></param></params>");
			sb.Append("</methodCall>");
			sb.Append("</message>");
			sb.Append("<message>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMethod2</methodName>");
			sb.Append("<params><param><value><string>test</string></value></param><param><value><boolean>0</boolean></value></param></params>");
			sb.Append("</methodCall>");
			sb.Append("</message>");
			sb.Append("</messageStream>");

			Console.Out.WriteLine("Generated:");
			Console.Out.WriteLine(s);

			Assert.AreEqual(sb.ToString(), s);
		}

		[Test]
		public void RequestMessageStreamDeserialize() {
			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
			sb.Append("<messageStream>");
			sb.Append("<message>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMethod1</methodName>");
			sb.Append("<params><param><value><boolean>1</boolean></value></param></params>");
			sb.Append("</methodCall>");
			sb.Append("</message>");
			sb.Append("<message>");
			sb.Append("<methodCall>");
			sb.Append("<methodName>testMethod2</methodName>");
			sb.Append("<params><param><value><string>test</string></value></param><param><value><boolean>0</boolean></value></param></params>");
			sb.Append("</methodCall>");
			sb.Append("</message>");
			sb.Append("</messageStream>");

			Message message = Deserialize(sb.ToString(), MessageType.Request);
			Assert.IsInstanceOf(typeof(MessageStream), message);
			MessageStream requestStream = (MessageStream) message;
			Assert.AreEqual(2, requestStream.MessageCount);
		}
	}
}
