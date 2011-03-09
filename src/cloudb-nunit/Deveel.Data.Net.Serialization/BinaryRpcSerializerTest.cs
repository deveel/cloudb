using System;
using System.IO;

using Deveel.Data.Net.Serialization;

using NUnit.Framework;

namespace Deveel.Data.Net.Client {
	[TestFixture]
	public sealed class BinaryyRcpMessageSerializerTest {
		private static readonly BinaryRpcMessageSerializer Serializer = new BinaryRpcMessageSerializer();

		private static Stream Serialize(Message message) {
			MemoryStream stream = new MemoryStream();
			Serializer.Serialize(message, stream);
			stream.Flush();
			return stream;
		}

		private static Message Deserialize(Stream stream, MessageType messageType) {
			stream.Seek(0, SeekOrigin.Begin);
			return Serializer.Deserialize(stream, messageType);
		}

		[Test]
		public void Test1() {
			RequestMessage request = new RequestMessage("testMethod1");
			request.Arguments.Add("string1");
			request.Arguments.Add(34L);
			request.Arguments.Add(new TcpServiceAddress("127.0.0.1", 3500));

			Stream stream = Serialize(request);

			Message message = Deserialize(stream, MessageType.Request);
			Assert.IsNotNull(message);
			Assert.AreEqual(MessageType.Request, message.MessageType);
			Assert.AreEqual("string1", message.Arguments[0].Value);
			Assert.AreEqual(34L, message.Arguments[1].Value);
			Assert.AreEqual(new TcpServiceAddress("127.0.0.1", 3500), message.Arguments[2].Value);
		}

		[Test]
		public void Test2() {
			ResponseMessage response = new ResponseMessage("response");
			IServiceAddress[] addresses = new IServiceAddress[1];
			int[] status = new int[1];
			response.Arguments.Add(1);
			addresses[0] = new TcpServiceAddress("127.0.0.1", 3500);
			status[0] = (int) ServiceStatus.Up;
			response.Arguments.Add(addresses);
			response.Arguments.Add(status);

			Stream stream = Serialize(response);

			Message message = Deserialize(stream, MessageType.Response);
			Assert.IsNotNull(message);
			Assert.AreEqual(MessageType.Response, message.MessageType);
			Assert.AreEqual(3, message.Arguments.Count);
			Assert.AreEqual(1, message.Arguments[0].Value);
			Assert.IsInstanceOf(typeof(IServiceAddress[]), message.Arguments[1].Value);
		}
	}
}