using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	public interface IPathTransaction : IDisposable {
		IPathContext Context { get; }


		DataAddress Commit();
	}
}