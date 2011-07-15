using System;
using System.Text;

namespace Deveel.Data.Net.Serialization {
	public abstract class XmlMessageSerializer : TextMessageSerializer {
		protected XmlMessageSerializer(string encoding)
			: base(encoding) {
		}

		protected XmlMessageSerializer(Encoding encoding)
			: base(encoding) {
		}

		protected XmlMessageSerializer() {
		}

		protected override string ContentType {
			get { return "text/xml"; }
		}
	}
}