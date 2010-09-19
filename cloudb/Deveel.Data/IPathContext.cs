using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	public interface IPathContext : IDisposable {
		string PathName { get; }

		NetworkClient Client { get; }


		IPathTransaction CreateTransaction();
	}
}