using System;
using System.IO;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net.Serialization {
	public interface IMessageSerializer {
		void Serialize(Message message, Stream output);

		Message Deserialize(Stream input, MessageType messageType);
	}
}