using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	[Handle(typeof(BasePath))]
	public sealed class BasePathMethodHandler : IMethodHandler {
		public IPathContext CreateContext(NetworkClient client, string pathName) {
			return new DbSession(client, pathName);
		}

		public MethodResponse HandleRequest(MethodRequest request) {
			throw new NotImplementedException();
		}
	}
}