using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public sealed class JsonRestActionSerializer : JsonActionSerializer {
		public override void DeserializeRequest(ActionRequest request, TextReader reader) {
			throw new NotImplementedException();
		}

		public override void SerializeResponse(ActionResponse response, TextWriter writer) {
			throw new NotImplementedException();
		}
	}
}