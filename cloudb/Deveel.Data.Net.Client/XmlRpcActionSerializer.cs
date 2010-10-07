using System;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public sealed class XmlRpcActionSerializer : XmlActionSerializer {
		public XmlRpcActionSerializer(string encoding)
			: base(encoding) {
		}

		public XmlRpcActionSerializer(Encoding encodig)
			: base(encodig) {
		}

		public XmlRpcActionSerializer() {
		}

		public override void DeserializeRequest(ActionRequest request, XmlReader reader) {
			throw new NotImplementedException();
		}

		public override void SerializeResponse(ActionResponse response, XmlWriter writer) {
			throw new NotImplementedException();
		}
	}
}