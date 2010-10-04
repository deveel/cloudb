using System;
using System.IO;

namespace Deveel.Data.Net {
	public interface IJsonSerializable {
		void SerializeJson(Stream output);

		void DeserializeJson(Stream input);
	}
}