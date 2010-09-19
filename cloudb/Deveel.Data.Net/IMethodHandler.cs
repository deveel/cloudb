using System;

namespace Deveel.Data.Net {
	public interface IMethodHandler {
		IPathContext CreateContext(NetworkClient client, string pathName);

		MethodResponse HandleRequest(MethodRequest request);
	}
}