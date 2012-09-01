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

		bool NotifyNodeChanged { get; }
		

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
		IList<ITreeNode> FetchNodes(NodeId[] nids);

		/// <summary>
		/// Checks if a node with the given identifier is cached.
		/// </summary>
		/// <param name="nodeId">The identifier of the node to check.</param>
		/// <returns>
		/// Returns <b>true</b> if the node identified was stored in the
		/// local cache, ot <b>false</b> otherwise.
		/// </returns>
		bool IsNodeAvailable(NodeId nodeId);

		/// <summary>
		/// Creates a shadow link to the leaf node with the given reference.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="id"></param>
		/// <remarks>
		/// A shadow link is a reference from a branch to a leaf that is already 
		/// linked to from another branch.
		/// <para>
		/// The number of <see cref="DisposeNode"/> operations needed to make a leaf 
		/// node eligible for reclamation is dependent on the number of shadow links 
		/// established on the leaf. If the implementation supports reference counting, 
		/// then this method should increment the reference count on the leaf node, and 
		/// <see cref="DisposeNode"/> should decrement the reference count. Assuming a 
		/// newly written node starts with a reference count of 1, once the reference 
		/// count is 0 the node resources can be reclaimed.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if establishing the shadow link was successful, or <b>false</b>
		/// if the shadow link was not possible either because the reference count reached 
		/// max capacity or shadow linking is not permitted.
		/// </returns>
		bool LinkLeaf(Key key, NodeId id);

		/// <summary>
		/// Disposes a node that was created or linked to the system.
		/// </summary>
		/// <param name="nid">The identifier of the node to dispose.</param>
		/// <remarks>
		/// When the mechanism of creation or linking of the nodes allows it,
		/// this method decrements the number of references to the node, untill
		/// total removal from the cache.
		/// </remarks>
		void DisposeNode(NodeId nid);

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
		IList<NodeId> Persist(TreeWrite write);
	}
}