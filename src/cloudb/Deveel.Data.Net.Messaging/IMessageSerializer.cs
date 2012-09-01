using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net.Messaging {
	public interface IMessageSerializer {
		void Serialize(IEnumerable<Message> message, Stream output);

		IEnumerable<Message> Deserialize(Stream input);
	}
}