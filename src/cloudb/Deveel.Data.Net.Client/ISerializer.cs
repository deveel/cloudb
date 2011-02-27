using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public interface ISerializer {
		bool CanSerialize(object obj);

		bool CanDeserialize(object obj);

		void Serialize(object obj, Stream output);

		object Deserialize(Stream stream);
	}
}