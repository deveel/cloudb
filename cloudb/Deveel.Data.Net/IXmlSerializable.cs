using System;
using System.IO;

namespace Deveel.Data.Net {
	public interface IXmlSerializable {
		void SerializeXml(Stream output);

		void DeserializeXml(Stream input);
	}
}