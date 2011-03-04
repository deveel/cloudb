using System;

namespace Deveel.Data.Net.Client {
	public interface IPathContext : IDisposable {
		string PathName { get; }
		
		NetworkClient Client { get; }


		IPathTransaction CreateTransaction();
	}
}