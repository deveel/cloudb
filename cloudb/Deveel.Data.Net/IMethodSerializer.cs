using System;
using System.IO;

namespace Deveel.Data.Net {
	public interface IMethodSerializer {
		void DeserializeRequest(MethodRequest request, Stream input);

		void SerializeResponse(MethodResponse response, Stream output);
	}
}