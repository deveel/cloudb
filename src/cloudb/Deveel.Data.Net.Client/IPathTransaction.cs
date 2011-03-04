using System;

namespace Deveel.Data.Net.Client {
	/// <summary>
	/// A client transaction opened on the path instance.
	/// </summary>
	public interface IPathTransaction : IDisposable {
		/// <summary>
		/// Gets the execution context of the path instance.
		/// </summary>
		IPathContext Context { get; }

		DataAddress Commit();
	}
}