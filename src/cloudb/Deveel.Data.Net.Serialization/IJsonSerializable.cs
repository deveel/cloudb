using System;
using System.IO;

namespace Deveel.Data.Net.Serialization {
	public interface IJsonSerializable {
		void SerializableJson(Stream output);

		void DeserializeJson(Stream input);
	}
}