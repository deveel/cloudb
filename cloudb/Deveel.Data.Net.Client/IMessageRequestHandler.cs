using System;

namespace Deveel.Data.Net.Client {
	public interface IMessageRequestHandler : IMessageProcessor {
		IPathContext CreateContext(NetworkClient client, string pathName);

		bool CanHandleClientType(string clientType);
	}
}