using System;
using System.Collections.Generic;

namespace Deveel.Data {
	/// <summary>
	/// Defines the mechanism for managing a tree-based storage system.
	/// </summary>
	/// <remarks>
	/// Implementations of this interfaces provide a flexible version
	/// managing system for the database storage.
	/// </remarks>
	public interface ITreeSystem {
		/// <summary>
		/// Gets the maximum number of branches in the tree.
		/// </summary>
		int MaxBranchSize { get; }

		/// <summary>
		/// Gets the maximum size in bytes for each leaf in a branch
		/// of the tree.
		/// </summary>
		int MaxLeafByteSize { get; }
		
		/// <summary>
		/// Gets the maximum size of the local transaction node heaps.
		/// </summary>
		long NodeHeapMaxSize { get; }
		

		/// <summary>
		/// Flushes all data stored in the local cache of the system
		/// to the underlying storage.
		/// </summary>
		void CheckPoint();

		/// <summary>
		/// Fetches a list of nodes from the tree corresponding to
		/// the specified identifiers.
		/// </summary>
		/// <param name="nids">An array of identifiers for the node to be fetched.</param>
		/// <returns>
		/// Returns a list of <see cref="ITreeNode"/> of the nodes fetched.
		/// </returns>
		IList<ITreeNode> FetchNodes(long[] nids);

		/// <summary>
		/// Checks if a node with the given identifier is cached.
		/// </summary>
		/// <param name="nodeId">The identifier of the node to check.</param>
		/// <returns>
		/// Returns <b>true</b> if the node identified was stored in the
		/// local cache, ot <b>false</b> otherwise.
		/// </returns>
		bool IsNodeAvailable(long nodeId);
		
		/// <summary>
		/// Creates a shadow link to the leaf identified.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="reference"></param>
		/// <remarks>
		/// A shadow link is a reference from a branch to a leaf that
		/// is already linked by another branch.
		/// <para>
		/// The number of <see cref="DisposeNode"/> operations needed to make 
		/// a leaf node eligible for reclaimation is dependant on the number 
		/// of shadow links established on the leaf.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if establishing the shadow link was successful, 
		/// othrwise <b>false</b> if the shadow link was not possible.
		/// </returns>
		bool LinkLeaf(Key key, long reference);

		/// <summary>
		/// Disposes a node that was created or linked to the system.
		/// </summary>
		/// <param name="nid">The identifier of the node to dispose.</param>
		/// <remarks>
		/// When the mechanism of creation or linking of the nodes allows it,
		/// this method decrements the number of references to the node, untill
		/// total removal from the cache.
		/// </remarks>
		void DisposeNode(long nid);

		/// <summary>
		/// Sets the current state of the tree in error.
		/// </summary>
		/// <param name="error">The error that represents the state of the tree.</param>
		/// <returns>
		/// </returns>
		ErrorStateException SetErrorState(Exception error);

		/// <summary>
		/// Checks if the tree is in an error-state and eventually throws
		/// an exception detailing the state.
		/// </summary>
		/// <see cref="SetErrorState"/>
		void CheckErrorState();

		/// <summary>
		/// Given a sequence of write operations, this method flushes the
		/// nodes present in the tree to the underlying storage, persisting
		/// the data.
		/// </summary>
		/// <param name="write">The object that specifies the operations to
		/// make for persisting the tree nodes into the underlying storage.</param>
		/// <returns>
		/// Returns a list of node identifiers for every node written to the 
		/// backing storage on the completion of the process.
		/// </returns>
		IList<long> Persist(TreeWrite write);
	}
}