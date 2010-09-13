using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	public sealed class BasePath : IPath {
		public void Init(IPathConnection connection) {
			throw new NotImplementedException();
		}

		public DataAddress Commit(IPathConnection connection, DataAddress rootNode) {
			throw new NotImplementedException();
		}
	}
}