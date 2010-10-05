using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public interface IXmlSerializable {
		void SerializeXml(Stream output);

		void DeserializeXml(Stream input);
	}
}