using System;
using System.IO;

namespace Deveel.Data.Net.Serialization {
	public interface IXmlSerializable {
		void SerializeXml(Stream output);

		void DeserializeXml(Stream input);
	}
}