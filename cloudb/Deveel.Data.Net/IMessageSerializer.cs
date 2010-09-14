using System;
using System.IO;

namespace Deveel.Data.Net {
	public interface IMessageSerializer {
		MessageStream Deserialize(Stream input);

		void Serialize(MessageStream messageStream, Stream output);
	}
}