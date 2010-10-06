using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public sealed class JsonRestMessageSerializer : JsonMessageSerializer {
		public override void Serialize(Message message, TextWriter writer) {
			throw new NotImplementedException();
		}

		public override void Deserialize(Message message, TextReader reader) {
			throw new NotImplementedException();
		}
	}
}