using System;

namespace Deveel.Data {
	/// <summary>
	/// Represents a single node in a tree system.
	/// </summary>
	public interface ITreeNode : IDisposable {
		NodeId Id { get; }

		/// <summary>
		/// Gets an estimation of the amount of memory needed by this 
		/// object to be managed by the process.
		/// </summary>
		/// <remarks>
		/// This value is used to determine the consumption of memory
		/// in the cache, and should be accurate.
		/// </remarks>
		long MemoryAmount { get; }
	}
}