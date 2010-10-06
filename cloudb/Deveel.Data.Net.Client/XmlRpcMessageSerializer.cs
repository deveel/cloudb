using System;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public sealed class XmlRpcMessageSerializer : XmlMessageSerializer {
		public XmlRpcMessageSerializer(string encoding)
			: base(encoding) {
		}

		public XmlRpcMessageSerializer(Encoding encodig)
			: base(encodig) {
		}

		public XmlRpcMessageSerializer() {
		}

		public override void Serialize(Message message, XmlWriter writer) {
			throw new NotImplementedException();
		}

		public override void Deserialize(Message message, XmlReader reader) {
			throw new NotImplementedException();
		}
	}
}