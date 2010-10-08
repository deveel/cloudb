using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public sealed class JsonRpcMessageSerializer : TextMessageSerializer, IMessageStreamSupport {
		protected override string ContentType {
			get { return "application/json"; }
		}
		
		protected override Message Deserialize(TextReader reader, MessageType messageType) {
			throw new NotImplementedException();
		}
		
		protected override void Serialize(Message message, TextWriter writer) {
			throw new NotImplementedException();
		}
	}
}