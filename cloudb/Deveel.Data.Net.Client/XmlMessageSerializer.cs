using System;
using System.IO;
using System.Text;
using System.Xml;

namespace Deveel.Data.Net.Client {
	public abstract class XmlMessageSerializer : TextMessageSerializer {
		private Encoding encoding;

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