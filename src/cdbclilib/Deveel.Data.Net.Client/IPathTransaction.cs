using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	public interface IPathTransaction : IDisposable {
		IPathClient Client { get; }


		IPathRequest CreateRequest();

		void Commit();
	}
}