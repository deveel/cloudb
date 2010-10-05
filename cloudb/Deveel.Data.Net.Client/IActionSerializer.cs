using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public interface IActionSerializer {
		void DeserializeRequest(ActionRequest request, Stream input);

		void SerializeResponse(ActionResponse response, Stream output);
	}
}