using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public interface IMessageSerializer {
		void Serialize(Message message, Stream output);

		void Deserialize(Message message, Stream input);
	}
}