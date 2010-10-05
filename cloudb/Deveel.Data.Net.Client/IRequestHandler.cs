using System;

namespace Deveel.Data.Net.Client {
	public interface IRequestHandler {
		IPathContext CreateContext(NetworkClient client, string pathName);

		ActionResponse HandleRequest(ActionRequest request);
	}
}