using System;

using Deveel.Data.Net;
using Deveel.Data.Net.Client;

namespace Deveel.Data {
	public interface IPathTransaction : IDisposable {
		IPathContext Context { get; }


		DataAddress Commit();
	}
}