using System;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public sealed class XmlRpcMessageSerializer : XmlMessageSerializer, IMessageStreamSupport {
		public XmlRpcMessageSerializer(string encoding)
			: base(encoding) {
		}

		public XmlRpcMessageSerializer(Encoding encodig)
			: base(encodig) {
		}

		public XmlRpcMessageSerializer() {
		}

		protected override Message Deserialize(XmlReader reader) {
			throw new NotImplementedException();
		}

		protected override void Serialize(Message message, XmlWriter writer) {
			throw new NotImplementedException();
		}
	}
}