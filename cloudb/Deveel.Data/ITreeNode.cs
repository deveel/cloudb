using System;

namespace Deveel.Data {
	/// <summary>
	/// Represents a single node in a tree system.
	/// </summary>
	public interface ITreeNode : IDisposable {
		/// <summary>
		/// Gets the identificator of the node within the tree.
		/// </summary>
		/// <remarks>
		/// If this number is smaller than 0, this means the node
		/// is resident in the memory and it is possible to change its
		/// contents (mutable). Otherwise, if this value is greater or 
		/// equalthan 0 this means it is in the store and immutable.
		/// </remarks>
		long Id { get; }

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