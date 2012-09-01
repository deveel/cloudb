using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Deveel.Data {
	public partial class TreeSystemTransaction : ITransaction {
		private NodeId rootNodeId;

		private readonly long versionId;

		private List<NodeId> nodeDeletes;
		private List<NodeId> nodeInserts;

		private TreeNodeHeap localNodeHeap;

		private readonly ITreeSystem treeStore;

		private long updateVersion;

		private Key lowestSizeChangedKey = Key.Tail;

		private int treeHeight = -1;

		private readonly Dictionary<Key, string> prefetchKeymap = new Dictionary<Key, string>();


		private bool readOnly;

		private bool disposed;
		private bool committed;
		private bool nonCommittable;

		public static readonly Key UserDataMin = new Key(Int16.MinValue, Int32.MinValue, Int64.MinValue + 1);
		public static readonly Key UserDataMax = new Key((short) 0x07F7F, Int32.MaxValue, Int64.MaxValue);


		public TreeSystemTransaction(ITreeSystem treeStore, long versionId, NodeId rootNodeId, bool readOnly) {
			this.treeStore = treeStore;
			this.rootNodeId = rootNodeId;
			this.versionId = versionId;
			updateVersion = 0;
			nodeDeletes = null;
			nodeInserts = null;
			this.readOnly = readOnly;
			disposed = false;
		}

		~TreeSystemTransaction() {
			Dispose(false);
		}

		private int MaxLeafByteSize {
			get { return treeStore.MaxLeafByteSize; }
		}

		private int MaxBranchSize {
			get { return treeStore.MaxBranchSize; }
		}

		private void FlushCache() {
			// When this is called, there should be no locks on anything related to
			// this object.

			// Manages the node cache
			NodeHeap.FlushCache();
		}

		private Key PreviousKeyOrder(Key key) {
			short type = key.Type;
			int secondary = key.Secondary;
			long primary = key.Primary;
			if (primary == Int64.MinValue) {
				// Should not use negative primary keys.
				throw new InvalidOperationException();
			}
			return new Key(type, secondary, primary - 1);
		}

		private ITreeNode FetchNodeIfLocallyAvailable(NodeId nodeId) {
			// If it's a heap node,
			if (IsHeapNode(nodeId)) {
				return FetchNode(nodeId);
			}

			// If the node is locally available, return it,
			if (treeStore.IsNodeAvailable(nodeId)) {
				return treeStore.FetchNodes(new NodeId[] {nodeId})[0];
			}
			// Otherwise return null
			return null;
		}

		private NodeId LastUncachedNode(Key key) {
			int curHeight = 1;
			NodeId childNodeId = RootNodeId;
			TreeBranch lastBranch = null;
			int childIndex = -1;

			// How this works;
			// * Descend through the tree and try to find the last node of the
			//   previous key.
			// * If a node is encoutered that is not cached locally, return it.
			// * If a leaf is reached, return the next leaf entry from the previous
			//   branch (this should be the first node of key).

			// This does not perform completely accurately for tree edges but this
			// should not present too much of a problem.

			key = PreviousKeyOrder(key);

			// Try and fetch the node, if it's not available locally then return the
			// child node ref
			ITreeNode node = FetchNodeIfLocallyAvailable(childNodeId);
			if (node == null) {
				return childNodeId;
			}

			while (true) {
				// Is the node a leaf?
				if (node is TreeLeaf) {
					treeHeight = curHeight;
					break;
				}

				// Must be a branch,
				TreeBranch branch = (TreeBranch) node;
				lastBranch = branch;
				// We ask the node for the child sub-tree that will contain this node
				childIndex = branch.SearchLast(key);
				// Child will be in this subtree
				childNodeId = branch.GetChild(childIndex);

				// Ok, if we know child_node_ref is a leaf,
				if (curHeight + 1 == treeHeight) {
					break;
				}

				// Try and fetch the node, if it's not available locally then return
				// the child node ref
				node = FetchNodeIfLocallyAvailable(childNodeId);
				if (node == null) {
					return childNodeId;
				}
				// Otherwise, descend to the child and repeat
				++curHeight;
			}

			// Ok, we've reached the end of the tree,

			// Fetch the next child_i if we are not at the end already,
			if (childIndex + 1 < lastBranch.ChildCount) {
				childNodeId = lastBranch.GetChild(childIndex);
			}

			// If the child node is not a heap node, and is not available locally then
			// return it.
			if (!IsHeapNode(childNodeId) &&
			    !treeStore.IsNodeAvailable(childNodeId)) {
				return childNodeId;
			}

			// The key is available locally,
			return null;
		}

		private void DiscoverPrefetchNodeSet(List<NodeId> nodeSet) {

			// If the map is empty, return
			if (prefetchKeymap.Count == 0) {
				return;
			}

			List<Key> keysToRemove = null;

			foreach (KeyValuePair<Key, string> pair in prefetchKeymap) {
				NodeId nodeId = LastUncachedNode(pair.Key);

				if (nodeId != null) {
					if (!nodeSet.Contains(nodeId)) {
						nodeSet.Add(nodeId);
					}					
				} else {
					if (keysToRemove == null)
						keysToRemove = new List<Key>();

					keysToRemove.Add(pair.Key);
				}
			}

			if (keysToRemove != null) {
				foreach (Key key in keysToRemove) {
					prefetchKeymap.Remove(key);
				}
			}
		}

		private static void ByteBufferCopyTo(IDataFile source, IDataFile target, long size) {
			long pos = target.Position;
			// Make room to insert the data
			target.Shift(size);
			target.Position = pos;
			// Set a 1k buffer
			byte[] buf = new byte[1024];
			// While there is data to copy,
			while (size > 0) {
				// Read an amount of data from the source
				int toRead = (int) Math.Min((long) buf.Length, size);
				// Read it into the buffer
				source.Read(buf, 0, toRead);
				// Write from the buffer out to the target
				target.Write(buf, 0, toRead);
				// Update the ref
				size = size - toRead;
			}
		}

		protected virtual void Dispose(bool disposing) {
			// If it's not already disposed,
			if (!disposed) {
				// Walk the tree and dispose all nodes on the heap,
				DisposeHeapNodes(RootNodeId);
				if (!committed) {
					// Then dispose all nodes that were inserted during the operation of
					// this transaction
					if (nodeInserts != null) {
						foreach (NodeId nodeId in nodeInserts) {
							ActualDisposeNode(nodeId);
						}
					}
				}
				// If this was committed then we don't dispose any nodes now but wait
				// until the version goes out of scope and then delete the nodes.  This
				// process is handled by the TreeSystem implementation.

				disposed = true;
			}

		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public IDataFile GetFile(Key key, FileAccess access) {
			CheckErrorState();
			try {

				// All key types greater than 0x07F80 are reserved for system data
				if (OutOfUserDataRange(key)) {
					throw new ArgumentException("Key is reserved for system data", "key");
				}

				// Use the unsafe method after checks have been performed
				return UnsafeGetDataFile(key, access);

			} catch (OutOfMemoryException e) {
				throw HandleMemoryException(e);
			}
		}

		public IDataRange GetRange(Key minKey, Key maxKey) {
			CheckErrorState();
			try {

				// All key types greater than 0x07F80 are reserved for system data
				if (OutOfUserDataRange(minKey) ||
					OutOfUserDataRange(maxKey)) {
					throw new ArgumentException("Key is reserved for system data");
				}

				// Use the unsafe method after checks have been performed
				return UnsafeGetDataRange(minKey, maxKey);

			} catch (OutOfMemoryException e) {
				throw HandleMemoryException(e);
			}
		}

		public IDataRange GetFullRange() {
			// The full range of user data
			return GetRange(UserDataMin, UserDataMax);
		}

		public void PreFetchKeys(Key[] keys) {
			CheckErrorState();

			try {
				foreach (Key k in keys) {
					prefetchKeymap.Add(k, "");
				}
			} catch (OutOfMemoryException e) {
				throw HandleMemoryException(e);
			}
		}

		internal void FlushNodesToStore(NodeId[] nodeIds) {
			// If not disposed,
			if (!disposed) {

				// Compact the entire tree
				object[] mergeBuffer = new object[5];
				CompactNode(Key.Head, RootNodeId, mergeBuffer, Key.Head, Key.Tail);

				// Flush the reference node list,
				RootNodeId = (FlushNodes(RootNodeId, nodeIds));

				// Update the version so any data file objects will flush with the
				// changes.
				++updateVersion;

				// Check out the changes
				treeStore.CheckPoint();
			}
		}

		internal ITreeNode FetchNode(NodeId nodeId) {
			// Is it a node we can fetch from the local node heap?
			if (IsHeapNode(nodeId)) {
				ITreeNode n = NodeHeap.FetchNode(nodeId);
				if (n == null)
					throw new NullReferenceException(nodeId.ToString());
				return n;
			}

			// If there's nothing in the prefetch keymap,
			if (prefetchKeymap.Count == 0) {
				ITreeNode n = treeStore.FetchNodes(new NodeId[] {nodeId})[0];
				if (n == null)
					throw new NullReferenceException(nodeId.ToString());
				return n;
			}

			List<NodeId> prefetchNodeset = new List<NodeId>();
			prefetchNodeset.Add(nodeId);
			DiscoverPrefetchNodeSet(prefetchNodeset);

			int len = prefetchNodeset.Count;
			NodeId[] nodeRefs = new NodeId[len];
			for (int i = 0; i < len; ++i) {
				nodeRefs[i] = prefetchNodeset[i];
			}

			{
				// Otherwise fetch the node from the tree store
				ITreeNode n = treeStore.FetchNodes(nodeRefs)[0];
				if (n == null)
					throw new NullReferenceException(nodeId.ToString());
				return n;
			}
		}

		private void LogStoreChange(byte type, NodeId pointer) {
			if (!treeStore.NotifyNodeChanged)
				return;

			// Special node type changes are not logged
			if (pointer.IsSpecial)
				return;

			if (type == 0) {
				// This type is for deleted nodes,
				NodeDeletes.Add(pointer);
			} else if (type == 1) {
				// This type is for inserts,
				NodeInserts.Add(pointer);
			} else {
				throw new ApplicationException("Incorrect type");
			}
		}

		public ITreeNode UnfreezeNode(ITreeNode node) {
			NodeId nodeId = node.Id;
			if (IsFrozen(nodeId)) {
				// Return a copy of the node
				ITreeNode newCopy = NodeHeap.Copy(node, treeStore.MaxBranchSize, treeStore.MaxLeafByteSize, this);
				// Delete the old node,
				DeleteNode(nodeId);
				return newCopy;
			}

			return node;
		}

		internal bool IsFrozen(NodeId nodeId) {
			return !nodeId.IsInMemory;
			//    // A node is frozen if either it is in the store (nodeId >= 0) or it has
			//    // the lock bit set to 0
			//    return nodeId >= 0 ||
			//           (nodeId & 0x02000000000000000L) == 0;
		}

		private int MergeNodes(Key middleKeyValue, NodeId leftId, NodeId rightId, Key leftLeftKey, Key rightLeftKey, object[] mergeBuffer) {
			// Fetch the nodes,
			ITreeNode leftNode = FetchNode(leftId);
			ITreeNode rightNode = FetchNode(rightId);
			// Are we merging branches or leafs?
			if (leftNode is TreeLeaf) {
				TreeLeaf lleaf = (TreeLeaf) leftNode;
				TreeLeaf rleaf = (TreeLeaf) rightNode;
				// Check the keys are identical,
				if (leftLeftKey.Equals(rightLeftKey)) {
					// 80% capacity on a leaf
					int capacity80 = (int) (0.80*MaxLeafByteSize);
					// True if it's possible to full merge left and right into a single
					bool fullyMerge = lleaf.Length + rleaf.Length <= MaxLeafByteSize;
					// Only proceed if the leafs can be fully merged or the left is less
					// than 80% full,
					if (fullyMerge || lleaf.Length < capacity80) {
						// Move elements from the right leaf to the left leaf so that either
						// the right node becomes completely empty or if that's not possible
						// the left node is 80% full.
						if (fullyMerge) {
							// We can fit both nodes into a single node so merge into a single
							// node,
							TreeLeaf nleaf = (TreeLeaf) UnfreezeNode(lleaf);
							byte[] copyBuf = new byte[rleaf.Length];
							rleaf.Read(0, copyBuf, 0, copyBuf.Length);
							nleaf.Write(nleaf.Length, copyBuf, 0, copyBuf.Length);

							// Delete the right node,
							DeleteNode(rleaf.Id);

							// Setup the merge state
							mergeBuffer[0] = nleaf.Id;
							mergeBuffer[1] = (long) nleaf.Length;
							return 1;
						}

						// Otherwise, we move bytes from the right leaf into the left
						// leaf until it is 80% full,
						int toCopy = capacity80 - lleaf.Length;
						// Make sure we are copying at least 4 bytes and there are enough
						// bytes available in the right leaf to make the copy,
						if (toCopy > 4 && rleaf.Length > toCopy) {
							// Unfreeze both the nodes,
							TreeLeaf mlleaf = (TreeLeaf) UnfreezeNode(lleaf);
							TreeLeaf mrleaf = (TreeLeaf) UnfreezeNode(rleaf);
							// Copy,
							byte[] copyBuf = new byte[toCopy];
							mrleaf.Read(0, copyBuf, 0, toCopy);
							mlleaf.Write(mlleaf.Length, copyBuf, 0, toCopy);
							// Shift the data in the right leaf,
							mrleaf.Shift(toCopy, -toCopy);

							// Return the merge state
							mergeBuffer[0] = mlleaf.Id;
							mergeBuffer[1] = (long) mlleaf.Length;
							mergeBuffer[2] = rightLeftKey;
							mergeBuffer[3] = mrleaf.Id;
							mergeBuffer[4] = (long) mrleaf.Length;
							return 2;
						}
					}
				} // leaf keys unequal
			} else if (leftNode is TreeBranch) {
				// Merge branches,
				TreeBranch lbranch = (TreeBranch) leftNode;
				TreeBranch rbranch = (TreeBranch) rightNode;

				int capacity75 = (int) (0.75*MaxBranchSize);
				// True if it's possible to full merge left and right into a single
				bool fullyMerge = lbranch.ChildCount + rbranch.ChildCount <= MaxBranchSize;

				// Only proceed if left is less than 75% full,
				if (fullyMerge || lbranch.ChildCount < capacity75) {
					// Move elements from the right branch to the left leaf only if the
					// branches can be completely merged into a node
					if (fullyMerge) {
						// We can fit both nodes into a single node so merge into a single
						// node,
						TreeBranch nbranch = (TreeBranch) UnfreezeNode(lbranch);
						// Merge,
						nbranch.MergeLeft(rbranch, middleKeyValue, rbranch.ChildCount);

						// Delete the right branch,
						DeleteNode(rbranch.Id);

						// Setup the merge state
						mergeBuffer[0] = nbranch.Id;
						mergeBuffer[1] = nbranch.LeafElementCount;
						return 1;
					}

					// Otherwise, we move children from the right branch into the left
					// branch until it is 75% full,
					int toCopy = capacity75 - lbranch.ChildCount;
					// Make sure we are copying at least 4 bytes and there are enough
					// bytes available in the right leaf to make the copy,
					if (toCopy > 2 && rbranch.ChildCount > toCopy + 3) {
						// Unfreeze the nodes,
						TreeBranch mlbranch = (TreeBranch) UnfreezeNode(lbranch);
						TreeBranch mrbranch = (TreeBranch) UnfreezeNode(rbranch);
						// And merge
						Key newMiddleValue = mlbranch.MergeLeft(mrbranch, middleKeyValue, toCopy);

						// Setup and return the merge state
						mergeBuffer[0] = mlbranch.Id;
						mergeBuffer[1] = mlbranch.LeafElementCount;
						mergeBuffer[2] = newMiddleValue;
						mergeBuffer[3] = mrbranch.Id;
						mergeBuffer[4] = mrbranch.LeafElementCount;
						return 2;
					}
				}
			} else {
				throw new ApplicationException("Unknown node type.");
			}

			// Signifies no change to the branch,
			return 3;
		}

		private void CompactNode(Key farLeft, NodeId id, object[] mergeBuffer, Key minBound, Key maxBound) {
			// If the ref is not on the heap, return the ref,
			if (!IsHeapNode(id))
				return;

			// Fetch the node,
			ITreeNode node = FetchNode(id);
			// If the node is a leaf, return the ref,
			if (node is TreeLeaf)
				return;

			// If the node is a branch,
			if (node is TreeBranch) {
				// Cast to a branch
				TreeBranch branch = (TreeBranch) node;

				// We ask the node for the child sub-tree that will contain the range
				// of this key
				int firstChildI = branch.SearchFirst(minBound);
				int lastChildI = branch.SearchLast(maxBound);

				// first_child_i may be negative which means a key reference is equal
				// to the key being searched, in which case we follow the left branch.
				if (firstChildI < 0) {
					firstChildI = -(firstChildI + 1);
				}

				// Compact the children,
				for (int x = firstChildI; x <= lastChildI; ++x) {
					// Change far left to represent the new far left node
					Key newFarLeft = (x > 0) ? branch.GetKey(x) : farLeft;

					// We don't change max_bound because it's not necessary.
					CompactNode(newFarLeft, branch.GetChild(x), mergeBuffer, minBound, maxBound);
				}

				// The number of children in this branch,
				int sz = branch.ChildCount;

				// Now try and merge the compacted children,
				int i = firstChildI;
				// We must not let there be less than 3 children
				while (sz > 3 && i <= lastChildI - 1) {
					// The left and right children nodes,
					NodeId leftChildId = branch.GetChild(i);
					NodeId rightChildId = branch.GetChild(i + 1);
					// If at least one of them is a heap node we attempt to merge the
					// nodes,
					if (IsHeapNode(leftChildId) || IsHeapNode(rightChildId)) {
						// Set the left left key and right left key of the references,
						Key leftLeftKey = (i > 0) ? branch.GetKey(i) : farLeft;
						Key rightLeftKey = branch.GetKey(i + 1);
						// Attempt to merge the nodes,
						int nodeResult = MergeNodes(branch.GetKey(i + 1),
						                             leftChildId, rightChildId,
						                             leftLeftKey, rightLeftKey,
						                             mergeBuffer);
						// If we merged into a single node then we update the left and
						// delete the right
						if (nodeResult == 1) {
							branch.SetChild((NodeId) mergeBuffer[0], i);
							branch.SetChildLeafElementCount((long) mergeBuffer[1], i);
							branch.RemoveChild(i + 1);
							// Reduce the size but don't increase i, because we may want to
							// merge again.
							--sz;
							--lastChildI;
						} else if (nodeResult == 2) {
							// Two result but there was a change (the left was increased in
							// size)
							branch.SetChild((NodeId) mergeBuffer[0], i);
							branch.SetChildLeafElementCount((long) mergeBuffer[1], i);
							branch.SetKeyToLeft((Key) mergeBuffer[2], i + 1);
							branch.SetChild((NodeId) mergeBuffer[3], i + 1);
							branch.SetChildLeafElementCount((long) mergeBuffer[4], i + 1);
							++i;
						} else {
							// Otherwise, no change so skip to the next child,
							++i;
						}
					}
						// left or right are not nodes on the heap so go to next,
					else {
						++i;
					}
				}
			}
		}

		private TreeBranch RecurseRebalanceTree(long leftOffset, int height, NodeId nodeId, long absolutePosition, Key inLeftKey) {
			// Put the node in memory,
			TreeBranch branch = (TreeBranch) FetchNode(nodeId);

			int sz = branch.ChildCount;
			int i;
			long pos = leftOffset;
			// Find the first child i that contains the position.
			for (i = 0; i < sz; ++i) {
				long childElemCount = branch.GetChildLeafElementCount(i);
				// abs position falls within bounds,
				if (absolutePosition >= pos &&
				    absolutePosition < pos + childElemCount) {
					break;
				}
				pos += childElemCount;
			}

			if (i > 0) {
				NodeId leftId = branch.GetChild(i - 1);
				NodeId rightId = branch.GetChild(i);

				// Only continue if both left and right are on the heap
				if (IsHeapNode(leftId) &&
				    IsHeapNode(rightId) &&
				    IsHeapNode(nodeId)) {

					Key leftKey = (i - 1 == 0)
					               	? inLeftKey
					               	: branch.GetKey(i - 1);
					Key rightKey = branch.GetKey(i);

					// Perform the merge operation,
					Key midKeyValue = rightKey;
					object[] mergeBuffer = new Object[5];
					int merge_result = MergeNodes(midKeyValue, leftId, rightId,
					                              leftKey, rightKey, mergeBuffer);
					if (merge_result == 1) {
						branch.SetChild((NodeId) mergeBuffer[0], i - 1);
						branch.SetChildLeafElementCount((long) mergeBuffer[1], i - 1);
						branch.RemoveChild(i);
					}
						//
					else if (merge_result == 2) {
						branch.SetChild((NodeId) mergeBuffer[0], i - 1);
						branch.SetChildLeafElementCount((long) mergeBuffer[1], i - 1);
						branch.SetKeyToLeft((Key) mergeBuffer[2], i);
						branch.SetChild((NodeId) mergeBuffer[3], i);
						branch.SetChildLeafElementCount((long) mergeBuffer[4], i);
					}
				}
			}

			// After merge, we don't know how the children will be placed, so we
			// do another search on the child to descend to,

			sz = branch.ChildCount;
			pos = leftOffset;
			// Find the first child i that contains the position.
			for (i = 0; i < sz; ++i) {
				long childElemCount = branch.GetChildLeafElementCount(i);
				// abs position falls within bounds,
				if (absolutePosition >= pos &&
				    absolutePosition < pos + childElemCount) {
					break;
				}
				pos += childElemCount;
			}

			// Descend on 'i'
			ITreeNode descendChild = FetchNode(branch.GetChild(i));

			// Finish if we hit a leaf
			if (descendChild is TreeLeaf) {
				// End if we hit the leaf,
				return branch;
			}

			Key newLeftKey = (i == 0)
			                   	? inLeftKey
			                   	: branch.GetKey(i);

			// Otherwise recurse on the child,
			TreeBranch child_branch =
				RecurseRebalanceTree(pos, height + 1,
				                     descendChild.Id, absolutePosition,
				                     newLeftKey);

			// Make sure we unfreeze the branch
			branch = (TreeBranch) UnfreezeNode(branch);

			// Update the child,
			branch.SetChild(child_branch.Id, i);
			branch.SetChildLeafElementCount(child_branch.LeafElementCount, i);

			// And return this branch,
			return branch;
		}

		private int PopulateSequence(NodeId id, TreeWrite sequence) {
			// If it's not a heap node, return
			if (!IsHeapNode(id))
				return -1;

			// It is a heap node, so fetch
			ITreeNode node = FetchNode(id);
			// Is it a leaf or a branch?
			if (node is TreeLeaf)
				// If it's a leaf, simply write it out
				return sequence.NodeWrite(node);

			if (node is TreeBranch) {
				// This is a branch,
				// Sequence this branch to be written out,
				int branchId = sequence.NodeWrite(node);
				// For each child in the branch,
				TreeBranch branch = (TreeBranch) node;
				int sz = branch.ChildCount;
				for (int i = 0; i < sz; ++i) {
					NodeId child = branch.GetChild(i);
					// Sequence the child
					int childId = PopulateSequence(child, sequence);
					// If something could be sequenced in the child,
					if (childId != -1) {
						// Make the branch command,
						sequence.BranchLink(branchId, i, childId);
					}
				}
				// Return the id of the branch in the sequence,
				return branchId;
			} else {
				throw new ApplicationException("Unknown node type.");
			}
		}

		internal bool IsHeapNode(NodeId nodeId) {
			return nodeId.IsInMemory;
		}

		internal NodeId WriteNode(NodeId nodeId) {
			// Create the sequence,
			TreeWrite sequence = new TreeWrite();
			// Create the command sequence to write this tree out,
			int rootId = PopulateSequence(nodeId, sequence);

			if (rootId != -1) {
				// Write out this sequence,
				IList<NodeId> refs = treeStore.Persist(sequence);

				// Update internal structure for each node written,
				IList<ITreeNode> nodes = sequence.BranchNodes;
				int sz = nodes.Count;
				for (int i = 0; i < sz; ++i) {
					WrittenNode(nodes[i], refs[i]);
				}
				int bnodesSz = sz;
				nodes = sequence.LeafNodes;
				sz = nodes.Count;
				for (int i = 0; i < sz; ++i) {
					WrittenNode(nodes[i], refs[i + bnodesSz]);
				}

				// Normalize the pointer,
				if (rootId >= TreeWrite.BranchPoint) {
					rootId = rootId - TreeWrite.BranchPoint;
				} else {
					rootId = rootId + bnodesSz;
				}

				// Return a reference to the node written,
				return refs[rootId];
			}
			return nodeId;
		}

		internal void DeleteNode(NodeId nodeId) {
			// If we are deleting a node that's on the temporary node heap, we delete
			// it immediately.  We know such nodes are only accessed within the scope of
			// this transaction so we can free up the resources immediately.

			// Is this a heap node?
			if (IsHeapNode(nodeId)) {
				// Delete it now
				NodeHeap.Delete(nodeId);
			} else {
				// Not a heap node, so we log that this node needs to be deleted when
				// we are certain it has gone out of scope of any concurrent transaction
				// that may need access to this data.
				// Logs a delete operation,
				LogStoreChange((byte)0, nodeId);
			}
		}

		private void WrittenNode(ITreeNode node, NodeId nodeId) {
			// Delete the reference to the old node,
			DeleteNode(node.Id);
			// Log the insert operation.
			LogStoreChange((byte) 1, nodeId);
		}

		private void ActualDisposeNode(NodeId nodeId) {
			// Dispose of the node,
			treeStore.DisposeNode(nodeId);
			// And return
		}

		private void DisposeHeapNodes(NodeId id) {
			// If it's not a heap node, return
			if (!IsHeapNode(id)) {
				return;
			}
			// It is a heap node, so fetch
			ITreeNode node = FetchNode(id);
			// Is it a leaf or a branch?
			if (node is TreeLeaf) {
				// If it's a leaf, dispose it
				DeleteNode(id);
				// And return,
				return;
			}

			if (node is TreeBranch) {
				// This is a branch, so we need to dipose the children if they are heap
				TreeBranch branch = (TreeBranch) node;

				int sz = branch.ChildCount;
				for (int i = 0; i < sz; ++i) {
					// Recurse for each child,
					DisposeHeapNodes(branch.GetChild(i));
				}
				// Then dispose this,
				DeleteNode(id);
				// And return,
				return;
			}

			throw new ApplicationException("Unknown node type.");
		}

		private void DisposeTree(NodeId id) {
			// It is a heap node, so fetch
			ITreeNode node = FetchNode(id);
			// Is it a leaf or a branch?
			if (node is TreeLeaf) {
				// If it's a leaf, dispose it
				DeleteNode(id);
				// And return,
				return;
			}

			if (node is TreeBranch) {
				// This is a branch, so we need to dipose the children if they are heap
				TreeBranch branch = (TreeBranch) node;

				int sz = branch.ChildCount;
				for (int i = 0; i < sz; ++i) {
					// Recurse for each child,
					DisposeTree(branch.GetChild(i));
				}
				// Then dispose this,
				DeleteNode(id);
				// And return,
				return;
			}

			throw new ApplicationException("Unknown node type.");
		}

		internal void RemoveAbsoluteBounds(long start, long end) {
			Object[] rv = RecurseRemoveBranches(0, 1, RootNodeId, start, end, Key.Head);
			RootNodeId = (NodeId)rv[0];
			long remove_count = (long)rv[1];

			// Assert we didn't remove more or less than requested,
			if (remove_count != (end - start)) {
				throw new ApplicationException("Assert failed " + remove_count + " to " + (end - start));
			}

			// Adjust position_end by the amount removed,
			end -= remove_count;

			// Rebalance the tree. This does not change the height of the tree but
			// it may leave single branch nodes at the top.
			RootNodeId = (RecurseRebalanceTree(0, 1, RootNodeId, end, Key.Head).Id);

			// Shrink the tree if the top contains single child branches
			while (true) {
				TreeBranch branch = (TreeBranch)FetchNode(RootNodeId);
				if (branch.ChildCount == 1) {
					// Delete the root node and go to the child,
					DeleteNode(RootNodeId);
					RootNodeId = branch.GetChild(0);
					if (TreeHeight != -1) {
						TreeHeight = TreeHeight - 1;
					}
				}
					// Otherwise break,
				else {
					break;
				}
			}

			// Done,
		}

		private long KeyEndPosition(Key key) {
			Key leftKey = Key.Head;
			int curHeight = 1;
			long leftOffset = 0;
			long nodeTotalSize = -1;
			ITreeNode node = FetchNode(RootNodeId);

			while (true) {
				// Is the node a leaf?
				if (node is TreeLeaf) {
					treeHeight = curHeight;
					break;
				}

				// Must be a branch,
				TreeBranch branch = (TreeBranch) node;
				// We ask the node for the child sub-tree that will contain this node
				int childIndex = branch.SearchLast(key);
				// Child will be in this subtree
				long childOffset = branch.GetChildOffset(childIndex);
				NodeId childNodeId = branch.GetChild(childIndex);
				nodeTotalSize = branch.GetChildLeafElementCount(childIndex);
				// Get the left key of the branch if we can
				if (childIndex > 0) {
					leftKey = branch.GetKey(childIndex);
				}
				// Update left_offset
				leftOffset += childOffset;

				// Ok, if we know child_node_ref is a leaf,
				if (curHeight + 1 == treeHeight) {
					break;
				}

				// Otherwise, descend to the child and repeat
				node = FetchNode(childNodeId);
				++curHeight;
			}

			// Ok, we've reached the end of the tree,
			// 'left_key' will be the key of the node we are on,
			// 'node_total_size' will be the size of the node,

			// If the key matches,
			int c = key.CompareTo(leftKey);
			if (c == 0)
				return leftOffset + nodeTotalSize;

			// If the searched for key is less than this
			if (c < 0)
				return -(leftOffset + 1);

			// If this key is greater, relative offset is at the end of this node.

			//if (c > 0) {
			return -((leftOffset + nodeTotalSize) + 1);
		}

		private long AbsKeyEndPosition(Key key) {
			long pos = KeyEndPosition(key);
			return (pos < 0) ? -(pos + 1) : pos;
		}

		private long[] GetDataFileBounds(Key key) {
			Key leftKey = Key.Head;
			int curHeight = 1;
			long leftOffset = 0;
			long nodeTotalSize = -1;
			ITreeNode node = FetchNode(RootNodeId);
			TreeBranch lastBranch = (TreeBranch) node;
			int childIndex = -1;

			while (true) {
				// Is the node a leaf?
				if (node is TreeLeaf) {
					treeHeight = curHeight;
					break;
				}

				// Must be a branch,
				TreeBranch branch = (TreeBranch) node;
				// We ask the node for the child sub-tree that will contain this node
				childIndex = branch.SearchLast(key);
				// Child will be in this subtree
				long childOffset = branch.GetChildOffset(childIndex);
				nodeTotalSize = branch.GetChildLeafElementCount(childIndex);
				// Get the left key of the branch if we can
				if (childIndex > 0) {
					leftKey = branch.GetKey(childIndex);
				}
				// Update left_offset
				leftOffset += childOffset;
				lastBranch = branch;

				// Ok, if we know child_node_ref is a leaf,
				if (curHeight + 1 == treeHeight) {
					break;
				}

				// Otherwise, descend to the child and repeat
				NodeId childNodeId = branch.GetChild(childIndex);
				node = FetchNode(childNodeId);
				++curHeight;
			}

			// Ok, we've reached the leaf node on the search,
			// 'left_key' will be the key of the node we are on,
			// 'node_total_size' will be the size of the node,
			// 'last_branch' will be the branch immediately above the leaf
			// 'child_i' will be the offset into the last branch we searched

			long endPos;

			// If the key matches,
			int c = key.CompareTo(leftKey);
			if (c == 0) {
				endPos = leftOffset + nodeTotalSize;
			}
				// If the searched for key is less than this
			else if (c < 0) {
				endPos = -(leftOffset + 1);
			}
				// If this key is greater, relative offset is at the end of this node.
			else {
				//if (c > 0) {
				endPos = -((leftOffset + nodeTotalSize) + 1);
			}

			// If the key doesn't exist return the bounds as the position data is
			// entered.
			if (endPos < 0) {
				long p = -(endPos + 1);
				return new long[] {p, p};
			}

			// Now we have the end position of a key that definitely exists, we can
			// query the parent branch and see if we can easily find the record
			// start.

			// Search back through the keys until we find a key that is different,
			// which is the start bounds of the key,
			long predictedStartPos = endPos - nodeTotalSize;
			for (int i = childIndex - 1; i > 0; --i) {
				Key k = lastBranch.GetKey(i);
				if (key.CompareTo(k) == 0) {
					// Equal,
					predictedStartPos = predictedStartPos - lastBranch.GetChildLeafElementCount(i);
				} else {
					// Not equal
					if (predictedStartPos > endPos) {
						throw new ApplicationException("Assertion failed: (1) start_pos > end_pos");
					}
					return new long[] {predictedStartPos, endPos};
				}
			}

			// Otherwise, find the end position of the previous key through a tree
			// search
			Key previousKey = PreviousKeyOrder(key);
			long startPos = AbsKeyEndPosition(previousKey);

			if (startPos > endPos) {
				throw new ApplicationException("Assertion failed: (2) start_pos > end_pos");
			}
			return new long[] {startPos, endPos};
		}

		internal TreeBranch CreateBranch() {
			return NodeHeap.CreateBranch(this, MaxBranchSize);
		}

		internal TreeLeaf CreateLeaf(Key key) {
			return NodeHeap.CreateLeaf(this, key, MaxLeafByteSize);
		}

		internal TreeLeaf CreateSparseLeaf(Key key, byte value, long length) {
			// Make sure the sparse leaf doesn't exceed the maximum leaf size
			int sparseSize = (int)Math.Min(length, (long)MaxLeafByteSize);
			// Make sure the sparse leaf doesn't exceed the maximum size of the
			// sparse leaf object.
			sparseSize = Math.Min(65535, sparseSize);

			// Create node reference for a special sparse node,
			NodeId nodeId = NodeId.CreateSpecialSparseNode(value, length);

			return (TreeLeaf)FetchNode(nodeId);
		}

		private NodeId FlushNodes(NodeId id, NodeId[] includeIds) {
			if (!IsHeapNode(id))
				return id;

			// Is this reference in the list?
			int c = Array.BinarySearch(includeIds, id);
			if (c < 0) {
				// It was not found, so go to the children,
				// Note that this node will change if it's a branch node, but the
				// reference to it will not change.

				// It is a heap node, so fetch
				ITreeNode node = FetchNode(id);
				// Is it a leaf or a branch?
				if (node is TreeLeaf)
					return id;

				if (node is TreeBranch) {
					// This is a branch, so we need to write out any children that are on
					// the heap before we write out the branch itself,
					TreeBranch branch = (TreeBranch) node;

					int sz = branch.ChildCount;

					for (int i = 0; i < sz; ++i) {
						NodeId oldId = branch.GetChild(i);
						// Recurse
						NodeId newId = FlushNodes(oldId, includeIds);
						branch.SetChild(newId, i);
					}
					// And return the reference
					return id;
				}

				throw new ApplicationException("Unknown node type.");
			}

			// This node was in the 'includeIds' list so write it out now,
			return WriteNode(id);
		}

		private void DeleteChildTree(int height, NodeId node) {
			if (height == treeHeight) {
				// This is a known leaf node,
				DeleteNode(node);
				return;
			}

			// Fetch the node,
			ITreeNode treeNode = FetchNode(node);
			if (treeNode is TreeLeaf) {
				// Leaf reached, so set the tree height, delete and return
				treeHeight = height;
				DeleteNode(node);
				return;
			}

			// The behaviour here changes depending on the system implementation.
			// Either we can simply unlink from the entire tree or we need to
			// recursely free all the leaf nodes.
			if (treeStore.NotifyNodeChanged) {
				// Need to account for all nodes so delete the node and all in the
				// sub-tree.
				DisposeTree(node);
			} else {
				// Otherwise we can simply unlink the branches on the heap and be
				// done with it.
				DisposeHeapNodes(node);
			}
		}

		private Object[] DeleteFromLeaf(long leftOffset, NodeId leaf, long startPos, long endPos, Key inLeftKey) {
			Debug.Assert(startPos < endPos);

			TreeLeaf treeLeaf = (TreeLeaf) UnfreezeNode(FetchNode(leaf));
			const int leafStart = 0;
			int leafEnd = treeLeaf.Length;
			int delStart = (int) Math.Max(startPos - leftOffset, (long) leafStart);
			int delEnd = (int) Math.Min(endPos - leftOffset, (long) leafEnd);

			int removeAmount = delEnd - delStart;

			// Remove from the end point,
			treeLeaf.Shift(delEnd, -removeAmount);

			return new object[] {
			                    	treeLeaf.Id,
			                    	(long) removeAmount,
			                    	inLeftKey, false
			                    };
		}

		private Object[] RecurseRemoveBranches(long leftOffset, int height, NodeId node, long startPos, long endPos, Key inLeftKey) {
			// Do we know if this is a leaf node?
			if (treeHeight == height)
				return DeleteFromLeaf(leftOffset, node, startPos, endPos, inLeftKey);

			// Fetch the node,
			ITreeNode treeNode = FetchNode(node);
			if (treeNode is TreeLeaf) {
				// Leaf reach, so set the tree height and return
				treeHeight = height;
				return DeleteFromLeaf(leftOffset, node, startPos, endPos, inLeftKey);
			}


			// The amount removed,
			long removeCount = 0;

			// This is a branch,
			TreeBranch treeBranch = (TreeBranch) treeNode;
			treeBranch = (TreeBranch) UnfreezeNode(treeBranch);

			Key parentLeftKey = inLeftKey;

			// Find all the children branches between the bounds,
			int childCount = treeBranch.ChildCount;
			long pos = leftOffset;
			for (int i = 0; i < childCount && pos < endPos; ++i) {
				long childNodeSize = treeBranch.GetChildLeafElementCount(i);
				long nextPos = pos + childNodeSize;

				// Test if start_pos/end_pos bounds intersects with this child,
				if (startPos < nextPos && endPos > pos) {
					// Yes, we intersect,

					// Make sure the branch is on the heap,
					NodeId childNode = treeBranch.GetChild(i);

					// If we intersect entirely remove the child from the branch,
					if (pos >= startPos && nextPos <= endPos) {
						// Delete the child tree,
						DeleteChildTree(height + 1, childNode);
						removeCount += childNodeSize;

						// If removing the first child, bubble up a new left_key
						if (i == 0) {
							parentLeftKey = treeBranch.GetKey(1);
						}
							// Otherwise parent left key doesn't change
						else {
							parentLeftKey = inLeftKey;
						}

						// Remove the child from the branch,
						treeBranch.RemoveChild(i);
						--i;
						--childCount;
					} else {
						// We don't intersect entirely, so recurse on this,
						// The left key
						Key rLeftKey = (i == 0) ? inLeftKey : treeBranch.GetKey(i);

						object[] rv = RecurseRemoveBranches(pos, height + 1, childNode, startPos, endPos, rLeftKey);
						NodeId newChildRef = (NodeId) rv[0];
						long removedInChild = (long) rv[1];
						Key childLeftKey = (Key) rv[2];

						removeCount += removedInChild;

						// Update the child,
						treeBranch.SetChild(newChildRef, i);
						treeBranch.SetChildLeafElementCount(childNodeSize - removedInChild, i);
						if (i == 0) {
							parentLeftKey = childLeftKey;
						} else {
							treeBranch.SetKeyToLeft(childLeftKey, i);
							parentLeftKey = inLeftKey;
						}
					}
				}

				// Next child in the branch,
				pos = nextPos;
			}

			// Return the reference and remove count,
			bool parentRebalance = (treeBranch.ChildCount <= 2);
			return new Object[] {
			                    	treeBranch.Id, removeCount,
			                    	parentLeftKey, parentRebalance
			                    };

		}

		private void CompactNodeKey(Key key) {
			Object[] mergeBuffer = new Object[5];
			// Compact the node,
			CompactNode(Key.Head, RootNodeId, mergeBuffer, key, key);
		}


		internal ITreeSystem TreeSystem {
			get { return treeStore; }
		}

		internal int TreeHeight {
			get { return treeHeight; }
			set { treeHeight = value; }
		}

		public NodeId RootNodeId {
			get { return rootNodeId; }
			set { rootNodeId = value; }
		}

		internal long VersionId {
			get { return versionId; }
		}

		public List<NodeId> NodeDeletes {
			get { return nodeDeletes ?? (nodeDeletes = new List<NodeId>(64)); }
		}

		public List<NodeId> NodeInserts {
			get { return nodeInserts ?? (nodeInserts = new List<NodeId>(64)); }
		}

		private TreeNodeHeap NodeHeap {
			get {
				// Note that we create the node heap on demand.  Transactions that only
				// read data will not incur this overhead.
				return localNodeHeap ?? (localNodeHeap = new TreeNodeHeap(13999, treeStore.NodeHeapMaxSize));
			}
		}

		public virtual void Checkout() {
			// Compact the entire tree,
			Object[] mergeBuffer = new Object[5];
			CompactNode(Key.Head, RootNodeId, mergeBuffer,
						Key.Head, Key.Tail);
			// Write out the changes
			RootNodeId = WriteNode(RootNodeId);

			// Update the version so any data file objects will flush with the
			// changes.
			++updateVersion;

			FlushCache();
		}

		protected virtual void SetToEmpty() {
			// Write a root node to the store,
			try {
				// Create an empty head node
				TreeLeaf headLeaf = CreateLeaf(Key.Head);
				// Insert a tree identification pattern
				headLeaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
				// Create an empty tail node
				TreeLeaf tailLeaf = CreateLeaf(Key.Tail);
				// Insert a tree identification pattern
				tailLeaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

				// Create a branch,
				TreeBranch rootBranch = CreateBranch();
				rootBranch.Set(headLeaf.Id, 4, Key.Tail, tailLeaf.Id, 4);

				RootNodeId = (rootBranch.Id);

			} catch (IOException e) {
				throw new SystemException(e.Message, e);
			}
		}

		protected internal virtual void OnCommitted() {
			if (nonCommittable) {
				throw new InvalidOperationException("Assertion failed, commit non-commitable.");
			}
			if (RootNodeId.IsInMemory) {
				throw new InvalidOperationException("Assertion failed, tree on heap.");
			}
			committed = true;
			readOnly = true;
		}

		protected IDataFile UnsafeGetDataFile(Key key, FileAccess mode) {
			// Check if the transaction disposed,
			if (disposed) {
				throw new InvalidOperationException("Transaction is disposed");
			}
			// Create and return the data file object for this key.
			return new DataFile(this, key, (mode == FileAccess.Read));
		}

		protected IDataRange UnsafeGetDataRange(Key minKey, Key maxKey) {
			// Check if the transaction disposed,
			if (disposed) {
				throw new InvalidOperationException("Transaction is disposed");
			}
			// Create and return the data file object for this key.
			return new DataRange(this, minKey, maxKey);
		}

		private void CheckErrorState() {
			treeStore.CheckErrorState();
		}

		private Exception HandleIOException(IOException e) {
			throw treeStore.SetErrorState(e);
		}

		private Exception HandleMemoryException(OutOfMemoryException e) {
			throw treeStore.SetErrorState(e);
		}


		public static bool OutOfUserDataRange(Key key) {
			// These types reserved for system use,
			if (key.Type >= (short) 0x07F80)
				return true;
			// Primary key has a reserved group of values at min value
			if (key.Primary <= Int64.MinValue + 16)
				return true;
			return false;
		}

		public bool DataFileExists(Key key) {
			CheckErrorState();

			try {

				// All key types above 0x07F80 are reserved for system data
				if (OutOfUserDataRange(key)) {
					throw new ApplicationException("Key is reserved for system data.");
				}
				// If the key exists, the position will be >= 0
				return KeyEndPosition(key) >= 0;

			} catch (IOException e) {
				throw HandleIOException(e);
			} catch (OutOfMemoryException e) {
				throw HandleMemoryException(e);
			}
		}
	}
}