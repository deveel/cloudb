using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data {
	internal class TreeSystemTransaction : ITransaction {
		private NodeId rootNodeId;
		private readonly long versionId;
		private List<NodeId> nodeDeletes;
		private List<NodeId> nodeInserts;
		private TreeNodeHeap nodeHeap;
		private readonly ITreeSystem storeSystem;
		private long updateVersion;
		private Key lowestSizeChangedKey = Key.Tail;
		private int treeHeight = -1;

		private readonly Dictionary<Key, String> prefetch_keymap = new Dictionary<Key, string>();

		private bool readOnly;
		private bool disposed;
		private bool committed;
		private bool non_committable;

		public static readonly Key UserDataMin = new Key(Int16.MinValue, Int32.MinValue, Int64.MinValue + 1);
		public static readonly Key UserDataMax = new Key((short) 0x07F7F, Int32.MaxValue, Int64.MaxValue);

		internal TreeSystemTransaction(ITreeSystem storeSystem, long versionId, NodeId rootNodeId, bool readOnly) {
			this.storeSystem = storeSystem;
			this.rootNodeId = rootNodeId;
			this.versionId = versionId;
			updateVersion = 0;
			nodeDeletes = null;
			nodeInserts = null;
			this.readOnly = readOnly;
			disposed = false;

		}

		public NodeId RootNodeId {
			get { return rootNodeId; }
			internal set { rootNodeId = value; }
		}

		public TreeNodeHeap NodeHeap {
			get {
				if (nodeHeap == null)
					nodeHeap = new TreeNodeHeap(13999, storeSystem.NodeHeapMaxSize);
				return nodeHeap;
			}
		}

		internal ITreeSystem TreeSystem {
			get { return storeSystem; }
		}

		internal int TreeHeight {
			get { return treeHeight; }
			set { treeHeight = value; }
		}

		private List<NodeId> NodeDeletes {
			get {
				if (nodeDeletes == null)
					nodeDeletes = new List<NodeId>(64);
				return nodeDeletes;
			}
		}

		private List<NodeId> NodeInserts {
			get {
				if (nodeInserts == null)
					nodeInserts = new List<NodeId>(64);
				return nodeInserts;
			}
		}

		internal IList<NodeId> DeletedNodes {
			get { return NodeDeletes; }
		}
		private int MaxLeafByteSize {
			get { return storeSystem.MaxLeafByteSize; }
		}

		private int MaxBranchSize {
			get { return storeSystem.MaxBranchSize; }
		}

		internal long VersionId {
			get { return versionId; }
		}

		public ICollection<Key> Keys {
			get { return new KeyCollection(this, GetRange()); }
		}


		internal TreeLeaf CreateSparseLeaf(Key key, byte b, long maxSize) {
			// Make sure the sparse leaf doesn't exceed the maximum leaf size
			int sparseSize = (int)Math.Min(maxSize, (long)MaxLeafByteSize);
			// Make sure the sparse leaf doesn't exceed the maximum size of the
			// sparse leaf object.
			sparseSize = Math.Min(65535, sparseSize);
			// Create node reference for a special sparse node,
			NodeId nodeId = NodeId.CreateSpecialSparseNode(b, maxSize);
			return (TreeLeaf)FetchNode(nodeId);
		}

		internal TreeLeaf CreateLeaf(Key key) {
			return NodeHeap.CreateLeaf(this, key, MaxLeafByteSize);
		}

		internal TreeBranch CreateBranch() {
			return NodeHeap.CreateBranch(this, MaxBranchSize);
		}

		internal static bool IsFrozen(NodeId nodeId) {
			return !nodeId.IsInMemory;
		}

		internal ITreeNode UnfreezeNode(ITreeNode node) {
			NodeId nodeId = node.Id;
			if (IsFrozen(nodeId)) {
				// Return a copy of the node
				ITreeNode newCopy = NodeHeap.Copy(node, storeSystem.MaxBranchSize,
												   storeSystem.MaxLeafByteSize, this);
				// Delete the old node,
				DeleteNode(nodeId);
				return newCopy;
			}
			return node;
		}

		internal ITreeNode FetchNode(NodeId nodeId) {
			// Is it a node we can fetch from the local node heap?
			if (IsHeapNode(nodeId)) {
				ITreeNode n = NodeHeap.FetchNode(nodeId);
				if (n == null)
					throw new NullReferenceException();
				return n;
			}

			// If there's nothing in the prefetch keymap,
			if (prefetch_keymap.Count == 0) {
				ITreeNode n = storeSystem.FetchNodes(new NodeId[] { nodeId })[0];
				if (n == null)
					throw new NullReferenceException();
				return n;
			}

			List<NodeId> prefetch_nodeset = new List<NodeId>();
			prefetch_nodeset.Add(nodeId);
			DiscoverPrefetchNodeSet(prefetch_nodeset);

			int len = prefetch_nodeset.Count;
			NodeId[] node_refs = new NodeId[len];
			for (int i = 0; i < len; ++i) {
				node_refs[i] = prefetch_nodeset[i];
			}

			// Otherwise fetch the node from the tree store
			ITreeNode node = storeSystem.FetchNodes(node_refs)[0];
			if (node == null)
				throw new NullReferenceException();
			return node;
		}

		private static Key PreviousKeyOrder(Key key) {
			short type = key.Type;
			int secondary = key.Secondary;
			long primary = key.Primary;
			if (primary == Int64.MinValue)
				throw new InvalidOperationException();

			return new Key(type, secondary, primary - 1);
		}


		private ITreeNode FetchNodeIfLocallyAvailable(NodeId nodeId) {
			// If it's a heap node,
			if (IsHeapNode(nodeId)) {
				return FetchNode(nodeId);
			}

			// If the node is locally available, return it,
			if (storeSystem.IsNodeAvailable(nodeId)) {
				return storeSystem.FetchNodes(new NodeId[] { nodeId })[0];
			}
			// Otherwise return null
			return null;
		}

		private NodeId LastUncachedNode(Key key) {
			int cur_height = 1;
			NodeId childNodeId = RootNodeId;
			TreeBranch lastBranch = null;
			int child_i = -1;

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
					treeHeight = cur_height;
					break;
				}
					// Must be a branch,
				else {
					TreeBranch branch = (TreeBranch)node;
					lastBranch = branch;
					// We ask the node for the child sub-tree that will contain this node
					child_i = branch.SearchLast(key);
					// Child will be in this subtree
					childNodeId = branch.GetChild(child_i);

					// Ok, if we know child_node_ref is a leaf,
					if (cur_height + 1 == treeHeight) {
						break;
					}

					// Try and fetch the node, if it's not available locally then return
					// the child node ref
					node = FetchNodeIfLocallyAvailable(childNodeId);
					if (node == null) {
						return childNodeId;
					}
					// Otherwise, descend to the child and repeat
					++cur_height;
				}
			}

			// Ok, we've reached the end of the tree,

			// Fetch the next child_i if we are not at the end already,
			if (child_i + 1 < lastBranch.ChildCount) {
				childNodeId = lastBranch.GetChild(child_i);
			}

			// If the child node is not a heap node, and is not available locally then
			// return it.
			if (!IsHeapNode(childNodeId) &&
				!storeSystem.IsNodeAvailable(childNodeId)) {
				return childNodeId;
			}
			// The key is available locally,
			return null;
		}

		private void DiscoverPrefetchNodeSet(IList<NodeId> nodeSet) {
			// If the map is empty, return
			if (prefetch_keymap.Count == 0) {
				return;
			}

			List<Key> toRemove = new List<Key>();
			foreach (Key key in prefetch_keymap.Keys) {
				NodeId nodeId = LastUncachedNode(key);

				if (nodeId != null) {
					if (!nodeSet.Contains(nodeId)) {
						nodeSet.Add(nodeId);
					}
				} else {
					// Remove the key from the prefetch map
					toRemove.Add(key);
				}
			}

			for (int i = toRemove.Count - 1; i >= 0; i--) {
				prefetch_keymap.Remove(toRemove[i]);
			}
		}

		internal static bool IsHeapNode(NodeId nodeId) {
			return nodeId.IsInMemory;
		}

		private static void ByteBufferCopyTo(DataFile source, DataFile target, long size) {
			long pos = target.Position;
			// Make room to insert the data
			target.Shift(size);
			target.Position = pos;
			// Set a 1k buffer
			byte[] buf = new byte[1024];
			// While there is data to copy,
			while (size > 0) {
				// Read an amount of data from the source
				int to_read = (int)System.Math.Min(buf.Length, size);
				// Read it into the buffer
				source.Read(buf, 0, to_read);
				// Write from the buffer out to the target
				target.Write(buf, 0, to_read);
				// Update the ref
				size = size - to_read;
			}
		}

		private int PopulateWrite(NodeId reference, TreeWrite write) {
			// If it's not a heap node, return
			if (!IsHeapNode(reference))
				return -1;

			// It is a heap node, so fetch
			ITreeNode node = FetchNode(reference);
			// Is it a leaf or a branch?
			if (node is TreeLeaf)
				// If it's a leaf, simply write it out
				return write.NodeWrite(node);
			if (node is TreeBranch) {
				// This is a branch,
				// Sequence this branch to be written out,
				int branch_id = write.NodeWrite(node);
				// For each child in the branch,
				TreeBranch branch = (TreeBranch)node;
				int sz = branch.ChildCount;
				for (int i = 0; i < sz; ++i) {
					NodeId child_ref = branch.GetChild(i);
					// Sequence the child
					int child_id = PopulateWrite(child_ref, write);
					// If something could be sequenced in the child,
					if (child_id != -1) {
						// Make the branch command,
						write.BranchLink(branch_id, i, child_id);
					}
				}
				// Return the id of the branch in the sequence,
				return branch_id;
			}

			throw new ApplicationException("Unknown node type.");
		}

		internal NodeId WriteNode(NodeId reference) {
			// Create the sequence,
			TreeWrite treeWrite = new TreeWrite();
			// Create the command sequence to write this tree out,
			int root_id = PopulateWrite(reference, treeWrite);

			if (root_id != -1) {
				// Write out this sequence,
				IList<NodeId> refs = storeSystem.Persist(treeWrite);

				// Update internal structure for each node written,
				IList<ITreeNode> nodes = treeWrite.BranchNodes;
				int sz = nodes.Count;
				for (int i = 0; i < sz; ++i) {
					OnNodeWritten(nodes[i], refs[i]);
				}
				int bnodes_sz = sz;
				nodes = treeWrite.LeafNodes;
				sz = nodes.Count;
				for (int i = 0; i < sz; ++i) {
					OnNodeWritten(nodes[i], refs[i + bnodes_sz]);
				}

				// Normalize the pointer,
				if (root_id >= TreeWrite.BranchPoint) {
					root_id = root_id - TreeWrite.BranchPoint;
				} else {
					root_id = root_id + bnodes_sz;
				}

				// Return a reference to the node written,
				return refs[root_id];
			} else {
				return reference;
			}
		}

		private void OnNodeWritten(ITreeNode node, NodeId reference) {
			// Delete the reference to the old node,
			DeleteNode(node.Id);
			// Log the insert operation.
			LogStoreChange(1, reference);
		}

		internal void DeleteNode(NodeId pointer) {
			// If we are deleting a node that's on the temporary node heap, we delete
			// it immediately.  We know such nodes are only accessed within the scope of
			// this transaction so we can free up the resources immediately.

			// Is this a heap node?
			if (IsHeapNode(pointer)) {
				// Delete it now
				NodeHeap.Delete(pointer);
			} else {
				// Not a heap node, so we log that this node needs to be deleted when
				// we are certain it has gone out of scope of any concurrent transaction
				// that may need access to this data.
				// Logs a delete operation,
				LogStoreChange(0, pointer);
			}
		}

		private void LogStoreChange(byte type, NodeId pointer) {
			if (!storeSystem.NotifyForAllNodes)
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
				throw new ArgumentException("Incorrect type");
			}
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
				TreeBranch branch = (TreeBranch)node;
				// We ask the node for the child sub-tree that will contain this node
				int child_i = branch.SearchLast(key);
				// Child will be in this subtree
				long childOffset = branch.GetChildOffset(child_i);
				NodeId childNodeId = branch.GetChild(child_i);
				nodeTotalSize = branch.GetChildLeafElementCount(child_i);
				// Get the left key of the branch if we can
				if (child_i > 0) {
					leftKey = branch.GetKey(child_i);
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
			return -((leftOffset + nodeTotalSize) + 1);
		}

		private long AbsKeyEndPosition(Key key) {
			long pos = KeyEndPosition(key);
			return (pos < 0) ? -(pos + 1) : pos;
		}

		private void GetDataFileBounds(Key key, out long start, out long end) {

			Key left_key = Key.Head;
			int cur_height = 1;
			long left_offset = 0;
			long node_total_size = -1;
			ITreeNode node = FetchNode(RootNodeId);
			TreeBranch last_branch = (TreeBranch) node;
			int child_i = -1;

			while (true) {
				// Is the node a leaf?
				if (node is TreeLeaf) {
					treeHeight = cur_height;
					break;
				}

				// Must be a branch,
				TreeBranch branch = (TreeBranch) node;
				// We ask the node for the child sub-tree that will contain this node
				child_i = branch.SearchLast(key);
				// Child will be in this subtree
				long child_offset = branch.GetChildOffset(child_i);
				node_total_size = branch.GetChildLeafElementCount(child_i);
				// Get the left key of the branch if we can
				if (child_i > 0) {
					left_key = branch.GetKey(child_i);
				}
				// Update left_offset
				left_offset += child_offset;
				last_branch = branch;

				// Ok, if we know child_node_ref is a leaf,
				if (cur_height + 1 == treeHeight) {
					break;
				}

				// Otherwise, descend to the child and repeat
				NodeId child_node_ref = branch.GetChild(child_i);
				node = FetchNode(child_node_ref);
				++cur_height;
			}

			// Ok, we've reached the leaf node on the search,
			// 'left_key' will be the key of the node we are on,
			// 'node_total_size' will be the size of the node,
			// 'last_branch' will be the branch immediately above the leaf
			// 'child_i' will be the offset into the last branch we searched

			long end_pos;

			// If the key matches,
			int c = key.CompareTo(left_key);
			if (c == 0) {
				end_pos = left_offset + node_total_size;
			}
				// If the searched for key is less than this
			else if (c < 0) {
				end_pos = -(left_offset + 1);
			}
				// If this key is greater, relative offset is at the end of this node.
			else {
				//if (c > 0) {
				end_pos = -((left_offset + node_total_size) + 1);
			}

			// If the key doesn't exist return the bounds as the position data is
			// entered.
			if (end_pos < 0) {
				long p = -(end_pos + 1);
				start = end = p;
				return;
			}

			// Now we have the end position of a key that definitely exists, we can
			// query the parent branch and see if we can easily find the record
			// start.

			// Search back through the keys until we find a key that is different,
			// which is the start bounds of the key,
			long predicted_start_pos = end_pos - node_total_size;
			for (int i = child_i - 1; i > 0; --i) {
				Key k = last_branch.GetKey(i);
				if (key.CompareTo(k) == 0) {
					// Equal,
					predicted_start_pos = predicted_start_pos -
					                      last_branch.GetChildLeafElementCount(i);
				} else {
					// Not equal
					if (predicted_start_pos > end_pos) {
						throw new ApplicationException("Assertion failed: (1) start_pos > end_pos");
					}
					start = predicted_start_pos;
					end = end_pos;
					return;
				}
			}

			// Otherwise, find the end position of the previous key through a tree
			// search
			Key previous_key = PreviousKeyOrder(key);
			long start_pos = AbsKeyEndPosition(previous_key);

			if (start_pos > end_pos) {
				throw new ApplicationException("Assertion failed: (2) start_pos > end_pos");
			}

			start = start_pos;
			end = end_pos;
		}

		private DataFile UnsafeGetDataFile(Key key, FileAccess access) {
			if (disposed)
				throw new ApplicationException("Transaction is disposed");

			return new TransactionDataFile(this, key, access);
		}

		internal IDataRange UnsafeGetDataRange(Key minKey, Key maxKey) {
			// Check if the transaction disposed,
			if (disposed)
				throw new ApplicationException("Transaction is disposed");

			// Create and return the data file object for this key.
			return new TransactionDataRange(this, minKey, maxKey);
		}

		private void CheckErrorState() {
			storeSystem.CheckErrorState();
		}

		private Exception SetErrorState(Exception error) {
			return storeSystem.SetErrorState(error);
		}

		private void DisposeNode(NodeId nodeId) {
			storeSystem.DisposeNode(nodeId);
		}

		private void DisposeHeapNodes(NodeId reference) {
			// If it's not a heap node, return
			if (!IsHeapNode(reference))
				return;

			// It is a heap node, so fetch
			ITreeNode node = FetchNode(reference);
			// Is it a leaf or a branch?
			if (node is TreeLeaf) {
				// If it's a leaf, dispose it
				DeleteNode(reference);
				// And return,
				return;
			}
			if (node is TreeBranch) {
				// This is a branch, so we need to dipose the children if they are heap
				TreeBranch branch = (TreeBranch)node;

				int sz = branch.ChildCount;
				for (int i = 0; i < sz; ++i) {
					// Recurse for each child,
					DisposeHeapNodes(branch.GetChild(i));
				}
				// Then dispose this,
				DeleteNode(reference);
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
			} else if (node is TreeBranch) {
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

		private void CompactNodeKey(Key key) {
			object [] mergeBuffer = new object[5];
			CompactNode(Key.Head, RootNodeId, mergeBuffer, key, key);
		}

		private void CompactNode(Key farLeft, NodeId reference, object[] mergeBuffer, Key minBound, Key maxBound) {
			// If the ref is not on the heap, return the ref,
			if (!IsHeapNode(reference))
				return;

			// Fetch the node,
			ITreeNode node = FetchNode(reference);
			// If the node is a leaf, return the ref,
			if (node is TreeLeaf)
				return;

			// If the node is a branch,
			if (node is TreeBranch) {
				// Cast to a branch
				TreeBranch branch = (TreeBranch)node;

				// We ask the node for the child sub-tree that will contain the range
				// of this key
				int first_child_i = branch.SearchFirst(minBound);
				int last_child_i = branch.SearchLast(maxBound);

				// first_child_i may be negative which means a key reference is equal
				// to the key being searched, in which case we follow the left branch.
				if (first_child_i < 0)
					first_child_i = -(first_child_i + 1);

				// Compact the children,
				for (int i = first_child_i; i <= last_child_i; ++i) {
					// Change far left to represent the new far left node
					Key new_far_left = (i > 0) ? branch.GetKey(i) : farLeft;

					// We don't change max_bound because it's not necessary.
					CompactNode(new_far_left, branch.GetChild(i), mergeBuffer, minBound, maxBound);
				}

				// The number of children in this branch,
				int sz = branch.ChildCount;

				// Now try and merge the compacted children,
				int i1 = first_child_i;
				// We must not let there be less than 3 children
				while (sz > 3 && i1 <= last_child_i - 1) {
					// The left and right children nodes,
					NodeId left_child_ref = branch.GetChild(i1);
					NodeId right_child_ref = branch.GetChild(i1 + 1);

					// If at least one of them is a heap node we attempt to merge the
					// nodes,
					if (IsHeapNode(left_child_ref) || IsHeapNode(right_child_ref)) {
						// Set the left left key and right left key of the references,
						Key left_left_key = (i1 > 0) ? branch.GetKey(i1) : farLeft;
						Key right_left_key = branch.GetKey(i1 + 1);
						// Attempt to merge the nodes,
						int node_result = MergeNodes(branch.GetKey(i1 + 1), left_child_ref, right_child_ref, left_left_key, right_left_key,
													 mergeBuffer);
						// If we merged into a single node then we update the left left and
						// delete the right
						if (node_result == 1) {
							branch.SetChild(i1, (NodeId) mergeBuffer[0]);
							branch.SetChildLeafElementCount(i1, (long)mergeBuffer[1]);
							branch.RemoveChild(i1 + 1);
							// Reduce the size but don't increase i, because we may want to
							// merge again.
							--sz;
							--last_child_i;
						} else if (node_result == 2) {
							// Two result but there was a change (the left was increased in
							// size)
							branch.SetChild(i1, (NodeId) mergeBuffer[0]);
							branch.SetChildLeafElementCount(i1, (long) mergeBuffer[1]);
							branch.SetKeyValueToLeft((Key) mergeBuffer[2], i1 + 1);
							branch.SetChild(i1 + 1, (NodeId) mergeBuffer[3]);
							branch.SetChildLeafElementCount(i1 + 1, (long) mergeBuffer[4]);
							++i1;
						} else {
							// Otherwise, no change so skip to the next child,
							++i1;
						}
					}
						// left or right are not nodes on the heap so go to next,
					else {
						++i1;
					}
				}
			}
		}

		private int MergeNodes(Key middleKeyValue, NodeId leftRef, NodeId rightRef, Key leftLeftKey, Key rightLeftKey, object [] mergeBuffer) {
			// Fetch the nodes,
			ITreeNode leftNode = FetchNode(leftRef);
			ITreeNode rightNode = FetchNode(rightRef);
			// Are we merging branches or leafs?
			if (leftNode is TreeLeaf) {
				TreeLeaf lleaf = (TreeLeaf)leftNode;
				TreeLeaf rleaf = (TreeLeaf)rightNode;
				// Check the keys are identical,
				if (leftLeftKey.Equals(rightLeftKey)) {
					int capacity80 = (int)(0.80 * MaxLeafByteSize);
					// True if it's possible to full merge left and right into a single
					bool fullyMerge = lleaf.Length + rleaf.Length <= MaxLeafByteSize;

					// Only proceed if left is less than 80% full,
					if (fullyMerge || lleaf.Length < capacity80) {
						// Move elements from the right leaf to the left leaf so that either
						// the right node becomes completely empty or if that's not possible
						// the left node is 80% full.
						if (fullyMerge) {
							// We can fit both nodes into a single node so merge into a single
							// node,
							TreeLeaf nleaf = (TreeLeaf)UnfreezeNode(lleaf);
							byte[] copy_buf = new byte[rleaf.Length];
							rleaf.Read(0, copy_buf, 0, copy_buf.Length);
							nleaf.Write(nleaf.Length, copy_buf, 0, copy_buf.Length);

							// Delete the right node,
							DeleteNode(rleaf.Id);

							// Setup the merge state
							mergeBuffer[0] = nleaf.Id;
							mergeBuffer[1] = (long) nleaf.Length;
							return 1;
						} else {
							// Otherwise, we move bytes from the right leaf into the left
							// leaf until it is 80% full,
							int to_copy = capacity80 - lleaf.Length;
							// Make sure we are copying at least 4 bytes and there are enough
							// bytes available in the right leaf to make the copy,
							if (to_copy > 4 && rleaf.Length > to_copy) {
								// Unfreeze both the nodes,
								TreeLeaf mlleaf = (TreeLeaf)UnfreezeNode(lleaf);
								TreeLeaf mrleaf = (TreeLeaf)UnfreezeNode(rleaf);
								// Copy,
								byte[] copyBuf = new byte[to_copy];
								mrleaf.Read(0, copyBuf, 0, to_copy);
								mlleaf.Read(mlleaf.Length, copyBuf, 0, to_copy);
								// Shift the data in the right leaf,
								mrleaf.Shift(to_copy, -to_copy);

								// Return the merge state
								mergeBuffer[0] = mlleaf.Id;
								mergeBuffer[1] = (long)mlleaf.Length;
								mergeBuffer[2] = rightLeftKey;
								mergeBuffer[3] = mrleaf.Id;
								mergeBuffer[4] = (long)mrleaf.Length;
								return 2;
							}
						}
					}
				} // leaf keys unequal
			} else if (leftNode is TreeBranch) {
				// Merge branches,
				TreeBranch lbranch = (TreeBranch)leftNode;
				TreeBranch rbranch = (TreeBranch)rightNode;

				int capacity75 = (int)(0.75 * MaxBranchSize);
				// True if it's possible to full merge left and right into a single
				bool fully_merge = lbranch.ChildCount + rbranch.ChildCount <= MaxBranchSize;
				// Only proceed if left is less than 75% full,
				if (fully_merge || lbranch.ChildCount < capacity75) {
					// Move elements from the right branch to the left leaf only if the
					// branches can be completely merged into a node
					if (fully_merge) {
						// We can fit both nodes into a single node so merge into a single
						// node,
						TreeBranch nbranch = (TreeBranch)UnfreezeNode(lbranch);
						// Merge,
						nbranch.MergeLeft(rbranch, middleKeyValue, rbranch.ChildCount);

						// Delete the right branch,
						DeleteNode(rbranch.Id);

						// Setup the merge state
						mergeBuffer[0] = nbranch.Id;
						mergeBuffer[1] = nbranch.LeafElementCount;
						return 1;
					} else {
						// Otherwise, we move children from the right branch into the left
						// branch until it is 75% full,
						int to_copy = capacity75 - lbranch.ChildCount;
						// Make sure we are copying at least 4 bytes and there are enough
						// bytes available in the right leaf to make the copy,
						if (to_copy > 2 && rbranch.ChildCount > to_copy + 3) {
							// Unfreeze the nodes,
							TreeBranch mlbranch = (TreeBranch)UnfreezeNode(lbranch);
							TreeBranch mrbranch = (TreeBranch)UnfreezeNode(rbranch);
							// And merge
							Key new_middle_value = mlbranch.MergeLeft(mrbranch, middleKeyValue, to_copy);

							// Setup and return the merge state
							mergeBuffer[0] = mlbranch.Id;
							mergeBuffer[1] = mlbranch.LeafElementCount;
							mergeBuffer[2] = new_middle_value;
							mergeBuffer[3] = mrbranch.Id;
							mergeBuffer[4] = mrbranch.LeafElementCount;
							return 2;
						}
					}
				}
			} else {
				throw new Exception("Unknown node type.");
			}
			// Signifies no change to the branch,
			return 3;
		}

		private TreeBranch RecurseRebalanceTree(long leftOffset, int height, NodeId nodeId, long absolutePosition, Key inLeftKey) {
			// Put the node in memory,
			TreeBranch branch = (TreeBranch) FetchNode(nodeId);

			int sz = branch.ChildCount;
			int i;
			long pos = leftOffset;
			// Find the first child i that contains the position.
			for (i = 0; i < sz; ++i) {
				long child_elem_count = branch.GetChildLeafElementCount(i);
				// abs position falls within bounds,
				if (absolutePosition >= pos &&
				    absolutePosition < pos + child_elem_count) {
					break;
				}
				pos += child_elem_count;
			}

			if (i > 0) {

				NodeId leftRef = branch.GetChild(i - 1);
				NodeId rightRef = branch.GetChild(i);

				// Only continue if both left and right are on the heap
				if (IsHeapNode(leftRef) &&
				    IsHeapNode(rightRef) &&
				    IsHeapNode(nodeId)) {

					Key leftKey = (i - 1 == 0) ? inLeftKey : branch.GetKey(i - 1);
					Key rightKey = branch.GetKey(i);

					// Perform the merge operation,
					Key midKeyValue = rightKey;
					Object[] mergeBuffer = new Object[5];
					int merge_result = MergeNodes(midKeyValue, leftRef, rightRef,
					                              leftKey, rightKey, mergeBuffer);
					if (merge_result == 1) {
						branch.SetChild(i - 1, (NodeId) mergeBuffer[0]);
						branch.SetChildLeafElementCount(i - 1, (long) mergeBuffer[1]);
						branch.RemoveChild(i);
					}
						//
					else if (merge_result == 2) {
						branch.SetChild(i - 1, (NodeId) mergeBuffer[0]);
						branch.SetChildLeafElementCount(i - 1, (long) mergeBuffer[1]);
						branch.SetKeyValueToLeft((Key) mergeBuffer[2], i);
						branch.SetChild(i, (NodeId) mergeBuffer[3]);
						branch.SetChildLeafElementCount(i, (long) mergeBuffer[4]);
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

			Key new_left_key = (i == 0) ? inLeftKey : branch.GetKey(i);

			// Otherwise recurse on the child,
			TreeBranch child_branch = RecurseRebalanceTree(pos, height + 1, descendChild.Id, absolutePosition, new_left_key);

			// Make sure we unfreeze the branch
			branch = (TreeBranch) UnfreezeNode(branch);

			// Update the child,
			branch.SetChild(i, child_branch.Id);
			branch.SetChildLeafElementCount(i, child_branch.LeafElementCount);

			// And return this branch,
			return branch;
		}

		private NodeId FlushNodes(NodeId nodeId, NodeId[] includeNodeIds) {
			if (!IsHeapNode(nodeId))
				return nodeId;

			// Is this reference in the list?
			int c = Array.BinarySearch(includeNodeIds, nodeId);
			if (c < 0) {
				// It was not found, so go to the children,
				// Note that this node will change if it's a branch node, but the
				// reference to it will not change.

				// It is a heap node, so fetch
				ITreeNode node = FetchNode(nodeId);
				// Is it a leaf or a branch?
				if (node is TreeLeaf)
					return nodeId;
				if (node is TreeBranch) {
					// This is a branch, so we need to write out any children that are on
					// the heap before we write out the branch itself,
					TreeBranch branch = (TreeBranch)node;

					int sz = branch.ChildCount;

					for (int i = 0; i < sz; ++i) {
						NodeId old_ref = branch.GetChild(i);
						// Recurse
						branch.SetChild(i, FlushNodes(old_ref, includeNodeIds));
					}
					// And return the reference
					return nodeId;
				}

				throw new Exception("Unknown node type.");
			}

			// This node was in the 'include_refs' list so write it out now,
			return WriteNode(nodeId);
		}

		private void FlushCache() {
			// When this is called, there should be no locks on anything related to
			// this object.
			NodeHeap.Flush();
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
			if (storeSystem.NotifyForAllNodes) {
				// Need to account for all nodes so delete the node and all in the
				// sub-tree.
				DisposeTree(node);
			} else {
				// Otherwise we can simply unlink the branches on the heap and be
				// done with it.
				DisposeHeapNodes(node);
			}
		}

		private object[] DeleteFromLeaf(long leftOffset, NodeId leaf, long startPos, long endPos, Key inLeftKey) {
			if (startPos < endPos)
				throw new ArgumentOutOfRangeException();

			TreeLeaf tree_leaf = (TreeLeaf) UnfreezeNode(FetchNode(leaf));
			int leaf_start = 0;
			int leaf_end = tree_leaf.Length;
			int del_start = (int) Math.Max(startPos - leftOffset, (long) leaf_start);
			int del_end = (int) Math.Min(endPos - leftOffset, (long) leaf_end);

			int remove_amount = del_end - del_start;

			// Remove from the end point,
			tree_leaf.Shift(del_end, -remove_amount);

			return new object[] {tree_leaf.Id, (long) remove_amount, inLeftKey, false};
		}

		private object[] RecurseRemoveBranches(long leftOffset, int height, NodeId node, long startPos, long endPos, Key inLeftKey) {
			// Do we know if this is a leaf node?
			if (treeHeight == height) {
				return DeleteFromLeaf(leftOffset, node, startPos, endPos, inLeftKey);
			}

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
						treeBranch.SetChild(i, newChildRef);
						treeBranch.SetChildLeafElementCount(i, childNodeSize - removedInChild);
						if (i == 0) {
							parentLeftKey = childLeftKey;
						} else {
							treeBranch.SetKeyValueToLeft(childLeftKey, i);
							parentLeftKey = inLeftKey;
						}
					}
				}

				// Next child in the branch,
				pos = nextPos;
			}

			// Return the reference and remove count,
			bool parentRebalance = (treeBranch.ChildCount <= 2);
			return new object[] { treeBranch.Id, removeCount, parentLeftKey, parentRebalance };
		}

		internal void RemoveAbsoluteBounds(long positionStart, long positionEnd) {
			// We scan from the root and remove branches that we determine are
			// fully represented by the key and bounds, being careful about edge
			// conditions.

			object[] rv = RecurseRemoveBranches(0, 1, RootNodeId, positionStart, positionEnd, Key.Head);
			RootNodeId = (NodeId) rv[0];
			long removeCount = (long) rv[1];

			// Assert we didn't remove more or less than requested,
			if (removeCount != (positionEnd - positionStart)) {
				throw new ApplicationException("Assert failed " + removeCount + " to " + (positionEnd - positionStart));
			}

			// Adjust position_end by the amount removed,
			positionEnd -= removeCount;

			// Rebalance the tree. This does not change the height of the tree but
			// it may leave single branch nodes at the top.
			RootNodeId = (RecurseRebalanceTree(0, 1, RootNodeId, positionEnd, Key.Head).Id);

			// Shrink the tree if the top contains single child branches
			while (true) {
				TreeBranch branch = (TreeBranch) FetchNode(RootNodeId);
				if (branch.ChildCount == 1) {
					// Delete the root node and go to the child,
					DeleteNode(RootNodeId);
					RootNodeId = (branch.GetChild(0));
					if (TreeHeight != -1) {
						TreeHeight = TreeHeight - 1;
					}
				}
					// Otherwise break,
				else {
					break;
				}
			}
		}

		#region Implementation of ITransaction

		public DataFile GetFile(Key key, FileAccess access) {
			CheckErrorState();
			try {
				if (OutOfUserDataRange(key))
					throw new ApplicationException("Key is reserved for system data.");

				return UnsafeGetDataFile(key, access);
			} catch (Exception e) {
				throw SetErrorState(e);
			}
		}

		public bool FileExists(Key key) {
			CheckErrorState();

			try {
				// All key types above 0x07F80 are reserved for system data
				if (OutOfUserDataRange(key))
					throw new ApplicationException("Key is reserved for system data.");

				// If the key exists, the position will be >= 0
				return KeyEndPosition(key) >= 0;
			} catch (IOException e) {
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				throw SetErrorState(e);
			}
		}

		public void PreFetchKeys(Key[] keys) {
			CheckErrorState();

			try {
				foreach (Key k in keys) {
					prefetch_keymap[k] = "";
				}
			} catch (OutOfMemoryException e) {
				throw SetErrorState(e);
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
				throw SetErrorState(e);
			}
		}

		public IDataRange GetRange() {
			// The full range of user data
			return GetRange(UserDataMin, UserDataMax);
		}

		#endregion

		protected void SetToEmpty() {
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

				RootNodeId = rootBranch.Id;
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			// If it's not already disposed,
			if (!disposed) {
				// Walk the tree and dispose all nodes on the heap,
				DisposeHeapNodes(RootNodeId);
				if (!committed) {
					// Then dispose all nodes that were inserted during the operation of
					// this transaction
					if (nodeInserts != null) {
						int sz = NodeInserts.Count;
						for (int i = 0; i < sz; ++i) {
							NodeId nodeId = NodeInserts[i];
							DisposeNode(nodeId);
						}
					}
				}
				// If this was committed then we don't dispose any nodes now but wait
				// until the version goes out of scope and then delete the nodes.  This
				// process is handled by the TreeSystem implementation.

				disposed = true;
			}
		}

		#endregion

		internal void FlushNodes(NodeId[] nids) {
			// If not disposed,
			if (!disposed) {
				// Compact the entire tree
				object[] mergeBuffer = new object[6];
				CompactNode(Key.Head, RootNodeId, mergeBuffer, Key.Head, Key.Tail);

				// Flush the reference node list,
				RootNodeId = FlushNodes(rootNodeId, nids);

				// Update the version so any data file objects will flush with the
				// changes.
				++updateVersion;

				// Check out the changes
				storeSystem.CheckPoint();
			}
		}

		internal void OnCommitted() {
			if (non_committable)
				throw new ApplicationException("Assertion failed, commit non-commitable.");
			if (RootNodeId.IsInMemory)
				throw new ApplicationException("Assertion failed, tree on heap.");
			committed = true;
			readOnly = true;
		}

		public virtual void CheckOut() {
			// Compact the entire tree,
			object[] mergeBuffer = new object[5];
			CompactNode(Key.Head, RootNodeId, mergeBuffer, Key.Head, Key.Tail);
			// Write out the changes
			RootNodeId = WriteNode(rootNodeId);

			// Update the version so any data file objects will flush with the
			// changes.
			++updateVersion;

			FlushCache();
		}

		public long FastSizeCalculate() {
			ITreeNode node = FetchNode(rootNodeId);
			if (node is TreeBranch) {
				TreeBranch branch = (TreeBranch)node;
				int sz = branch.ChildCount;
				// Add up the sizes of the children in the branch
				long r_size = 0;
				for (int i = 0; i < sz; ++i) {
					r_size += branch.GetChildLeafElementCount(i);
				}
				return r_size;
			}

			TreeLeaf leaf = (TreeLeaf)node;
			return leaf.Length;
		}

		public static bool OutOfUserDataRange(Key key) {
			// These types reserved for system use,
			if (key.Type >= Key.SpecialKeyType)
				return true;

			// Primary key has a reserved group of values at min value
			if (key.Primary <= Int64.MinValue + 16)
				return true;

			return false;
		}

		#region TransactionDataFile

		private class TransactionDataFile : DataFile {
			private readonly TreeSystemTransaction tnx;
			private FileAccess access;
			private Key key;
			private long pos;

			private long version;
			private long start;
			private long end;

			private TreeSystemStack stack;

			internal TransactionDataFile(TreeSystemTransaction tnx, Key key, FileAccess access) {
				this.tnx = tnx;
				stack = new TreeSystemStack(tnx);
				this.key = key;
				pos = 0;

				version = -1;
				this.access = access;
				start = -1;
				end = -1;
			}

			private ITreeSystem TreeSystem {
				get { return tnx.storeSystem; }
			}

			private TreeSystemTransaction Transaction {
				get { return tnx; }
			}

			private void EnsureCorrectBounds() {
				if (tnx.updateVersion > version) {

					// If version is -1, we force a key position lookup.  Version is -1
					// when the file is created or it undergoes a large structural change
					// such as a copy.
					if (version == -1 || key.CompareTo(tnx.lowestSizeChangedKey) >= 0) {
						tnx.GetDataFileBounds(key, out start, out end);
					}
					// Reset the stack and set the version to the most recent
					stack.Reset();
					version = tnx.updateVersion;
				}
			}

			private void EnsureBounds(long end_point) {
				// The number of bytes to expand by
				long to_expand_by = end_point - end;

				// If we need to expand,
				if (to_expand_by > 0) {
					long size_diff = to_expand_by;
					// Go to the end position,
					stack.SetupForPosition(key, Math.Max(start, end - 1));
					// Did we find a leaf for this key?
					if (!stack.CurrentLeafKey.Equals(key)) {
						// No, so add empty nodes after to make up the space
						stack.AddSpaceAfter(key, to_expand_by);
					} else {
						// Otherwise, try to expand the current leaf,
						to_expand_by -= stack.ExpandLeaf(to_expand_by);
						// And add nodes for the remaining
						stack.AddSpaceAfter(key, to_expand_by);
					}
					end = end_point;

					// Update the state because this key changed the relative offset of
					// the keys ahead of it.
					UpdateLowestSizeChangedKey();
				}
			}

			private void ShiftData(long position, long shift_offset) {
				// Make some assertions
				long end_pos = position + shift_offset;
				if (position < start || position > end)
					throw new IndexOutOfRangeException("Position is out of bounds.");

				// Make sure the ending position can't be before the start
				if (end_pos < start)
					throw new IndexOutOfRangeException("Cannot shift to before start boundary.");

				stack.ShiftData(key, position, shift_offset);
				end += shift_offset;
				if (end < start)
					throw new ApplicationException("Assertion failed: end < start");

				// Update the state because this key changed the relative offset of
				// the keys ahead of it.
				UpdateLowestSizeChangedKey();
			}
			private void UpdateLowestSizeChangedKey() {
				// Update the lowest sized changed key
				if (key.CompareTo(tnx.lowestSizeChangedKey) < 0) {
					tnx.lowestSizeChangedKey = key;
				}
			}

			private void CheckAccessSize(int len) {
				if (pos < 0 || pos > (end - start - len))
					throw new ApplicationException("position out of bounds.");
			}

			private void CompactNodeKey(Key key) {
				tnx.CompactNodeKey(key);
			}

			private void InitWrite() {
				// Generate exception if this is read-only.
				// Either the transaction is read only or the file is read only
				if (tnx.readOnly)
					throw new ApplicationException("Read only transaction.");
				if (access == FileAccess.Read)
					throw new ApplicationException("Read only data file.");

				// On writing, we update the versions
				if (version >= 0) {
					++version;
				}
				++tnx.updateVersion;
			}

			private void CopyDataTo(long position, TransactionDataFile target_data_file, long target_position, long size) {
				// If transactions are the same (data is being copied within the same
				// transaction context).
				TreeSystemStack target_stack;
				TreeSystemStack source_stack;
				// Keys
				Key target_key = target_data_file.key;
				Key source_key = key;

				bool modify_pos_on_shift = false;
				if (target_data_file.Transaction == Transaction) {
					// We set the source and target stack to the same
					source_stack = target_data_file.stack;
					target_stack = source_stack;
					// If same transaction and target_position is before the position we
					// set the modify_pos_on_shift boolean.  This will update the absolute
					// position when data is copied.
					modify_pos_on_shift = (target_position <= position);
				} else {
					// Otherwise, set the target stack to the target file's stack
					source_stack = stack;
					target_stack = target_data_file.stack;
				}


				// Compact the key we are copying from, and in the destination,
				CompactNodeKey(source_key);
				target_data_file.CompactNodeKey(target_key);


				// The process works as follows;
				// 1. If we are not positioned at the start of a leaf, copy all data up
				//    to the next leaf to the target.
				// 2. Split the target leaf at the new position if the leaf can be
				//    split into 2 leaf nodes.
				// 3. Copy every full leaf to the target as a new leaf element.
				// 4. If there is any remaining data to copy, insert it into the target.

				// Set up for the position
				source_stack.SetupForPosition(source_key, position);
				// If we aren't at the start of the leaf, then copy the data to the
				// target.
				int leaf_off = source_stack.LeafOffset;
				if (leaf_off > 0) {
					// We copy the remaining data in the leaf to the target
					// The amount of data to copy from the leaf to the target
					int to_copy = (int)Math.Min(size, source_stack.LeafSize - leaf_off);
					if (to_copy > 0) {
						// Read into a buffer
						byte[] buf = new byte[to_copy];
						source_stack.CurrentLeaf.Read(leaf_off, buf, 0, to_copy);
						// Make enough room to insert this data in the target
						target_stack.ShiftData(target_key, target_position, to_copy);
						// Update the position if necessary
						if (modify_pos_on_shift) {
							position += to_copy;
						}
						// Write the data to the target stack
						target_stack.WriteFrom(target_key, target_position, buf, 0, to_copy);
						// Increment the pointers
						position += to_copy;
						target_position += to_copy;
						size -= to_copy;
					}
				}

				// If this is true, the next iteration will use the byte buffer leaf copy
				// routine.  Set if a link to a node failed for whatever reason.
				bool use_byte_buffer_copy_for_next = false;

				// The loop
				while (size > 0) {
					// We now know we are at the start of a leaf with data left to copy.
					source_stack.SetupForPosition(source_key, position);
					// Lets assert that
					if (source_stack.LeafOffset != 0) {
						throw new Exception("Expected to be at the start of a leaf.");
					}

					// If the source is a heap node or we are copying less than the data
					// that's in the leaf then we use the standard shift and write.
					TreeLeaf current_leaf = source_stack.CurrentLeaf;
					// Check the leaf size isn't 0
					if (current_leaf.Length <= 0)
						throw new Exception("Leaf is empty.");

					// If the remaining copy is less than the size of the leaf we are
					// copying from, we just do a byte array copy
					if (use_byte_buffer_copy_for_next || size < current_leaf.Length) {
						use_byte_buffer_copy_for_next = false;
						int to_copy = (int)Math.Min(size, current_leaf.Length);
						// Read into a buffer
						byte[] buf = new byte[to_copy];
						current_leaf.Read(0, buf, 0, to_copy);
						// Make enough room in the target
						target_stack.ShiftData(target_key, target_position, to_copy);
						if (modify_pos_on_shift) {
							position += to_copy;
						}
						// Write the data and finish
						target_stack.WriteFrom(target_key, target_position, buf, 0, to_copy);
						// Update pointers
						position += to_copy;
						target_position += to_copy;
						size -= to_copy;
					} else {
						// We need to copy a complete leaf node,
						// If the leaf is on the heap, write it out
						if (IsHeapNode(current_leaf.Id)) {
							source_stack.WriteLeafOnly(source_key);
							// And update any vars
							current_leaf = source_stack.CurrentLeaf;
						}

						// Ok, source current leaf isn't on the heap, and we are copying a
						// complete leaf node, so we are elegible to play with pointers to
						// copy the data.
						target_stack.SetupForPosition(target_key, target_position);
						bool insert_next_before = false;
						// Does the target key exist?
						bool target_key_exists = target_stack.CurrentLeafKey.Equals(target_key);
						if (target_key_exists) {
							// If the key exists, is target_position at the end of the span?
							insert_next_before = target_stack.LeafOffset < target_stack.CurrentLeaf.Length;
						}

						// If target isn't currently on a boundary
						if (!target_stack.IsAtEndOfKeyData && target_stack.LeafOffset != 0) {
							// If we aren't on a boundary we need to split the target leaf
							target_stack.SplitLeaf(target_key, target_position);
						}
						// If the key exists we set up the position to the previous left
						// to insert the new leaf, otherwise we set it up to the default
						// position to insert.

						// Copy the leaf,
						// Try to link to this leaf
						bool link_successful = TreeSystem.LinkLeaf(target_key, current_leaf.Id);
						// If the link was successful,
						if (link_successful) {
							// Insert the leaf into the tree
							target_stack.InsertLeaf(target_key, current_leaf, insert_next_before);
							// Update the pointers
							int copied_size = current_leaf.Length;
							// Update if we inserting stuff before
							if (modify_pos_on_shift) {
								position += copied_size;
							}
							position += copied_size;
							target_position += copied_size;
							size -= copied_size;
						}
							// If the link was not successful,
						else {
							// We loop back and use the byte buffer copy,
							use_byte_buffer_copy_for_next = true;
						}
					}
				}
			}

			#region Implementation of IDataFile

			public override long Length {
				get {
					tnx.CheckErrorState();
					try {
						EnsureCorrectBounds();
						return (end - start);
					} catch (Exception e) {
						throw tnx.SetErrorState(e);
					}
				}
			}

			public override long Position {
				get { return pos; }
				set { pos = value; }
			}

			public override int Read(byte[] buffer, int offset, int count) {
				tnx.CheckErrorState();
				try {
					EnsureCorrectBounds();
					CheckAccessSize(count);
					int readCount = stack.ReadInto(key, start + pos, buffer, offset, count);
					pos += readCount;
					return readCount;
				} catch (Exception e) {
					throw tnx.SetErrorState(e);
				}
			}

			public override void Write(byte[] buffer, int offset, int count) {
				tnx.CheckErrorState();
				try {
					InitWrite();
					EnsureCorrectBounds();
					CheckAccessSize(0);

					// Ensure that there is address space available for writing this.
					EnsureBounds(start + pos + count);
					stack.WriteFrom(key, start + pos, buffer, offset, count);
					pos += count;

					tnx.NodeHeap.Flush();
				} catch (Exception e) {
					throw tnx.SetErrorState(e);
				}
			}

			public override void SetLength(long value) {
				tnx.CheckErrorState();

				try {
					InitWrite();
					EnsureCorrectBounds();

					long current_size = end - start;
					ShiftData(end, value - current_size);

					tnx.FlushCache();
				} catch (IOException e) {
					throw tnx.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw tnx.SetErrorState(e);
				}
			}

			public override void Shift(long offset) {
				tnx.CheckErrorState();

				try {
					InitWrite();
					EnsureCorrectBounds();
					CheckAccessSize(0);

					ShiftData(start + pos, offset);

					tnx.FlushCache();
				} catch (IOException e) {
					throw tnx.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw tnx.SetErrorState(e);
				}
			}

			public override void Delete() {
				tnx.CheckErrorState();

				try {
					InitWrite();
					EnsureCorrectBounds();

					ShiftData(end, start - end);

					tnx.FlushCache();
				} catch (IOException e) {
					throw tnx.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw tnx.SetErrorState(e);
				}
			}

			public override void CopyTo(DataFile destFile, long size) {
				tnx.CheckErrorState();

				try {
					// The actual amount of data to really copy
					size = Math.Min(Length - Position, size);
					// Return if we aren't doing anything
					if (size <= 0)
						return;

					// If the target isn't a TranDataFile then use standard byte buffer copy.
					if (!(destFile is TransactionDataFile)) {
						ByteBufferCopyTo(this, destFile, size);
						return;
					}
					// If the tree systems are different, then byte buffer copy.
					TransactionDataFile t_target = (TransactionDataFile)destFile;
					if (TreeSystem != t_target.TreeSystem) {
						ByteBufferCopyTo(this, destFile, size);
						return;
					}
					// Fail condition (same key and same transaction),
					if (t_target.key.Equals(key) &&
						t_target.Transaction == Transaction) {
						throw new Exception("Can not copy data within a file.");
					}

					// InitWrite on this and target.  The reason we do this is because we may
					// change the root node on either source or target.  We need to
					// InitWrite on this object even though the data may not change, because
					// we may be writing out data from the heap as part of the copy operation
					// and the root node may change
					InitWrite();
					t_target.InitWrite();

					// Make sure internal vars are setup correctly
					EnsureCorrectBounds();
					t_target.EnsureCorrectBounds();

					// Remember the source and target positions
					long init_spos = Position;
					long init_tpos = t_target.Position;

					// Ok, the target shares the same tree system, therefore we may be able
					// to optimize the copy.
					CopyDataTo(start + Position, t_target, t_target.start + t_target.Position, size);

					// Update the positions
					Position = init_spos + size;
					t_target.Position = init_tpos + size;

					// Reset version to force a bound update
					version = -1;
					t_target.version = -1;
					t_target.UpdateLowestSizeChangedKey();
					t_target.Transaction.FlushCache();
				} catch (IOException e) {
					throw tnx.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw tnx.SetErrorState(e);
				}
			}

			public override void ReplicateTo(DataFile target) {
				// TODO: Placeholder implementation,
				target.Position = 0;
				target.Delete();
				Position = 0;
				CopyTo(target, Length);
			}

			#endregion
		}

		#endregion

		#region TransactionDataRange

		private class TransactionDataRange : IDataRange {
			private readonly TreeSystemTransaction transaction;
			// The lower and upper bounds of the range
			private readonly Key lower_key;
			private readonly Key upper_key;

			// The current absolute position
			private long p;

			// The current version of the bounds information.  If it is out of date
			// it must be updated.
			private long version;
			// The current absolute start position
			private long start;
			// The current absolute position (changes when modification happens)
			private long end;

			// Tree stack
			private readonly TreeSystemStack stack;



			public TransactionDataRange(TreeSystemTransaction transaction, Key lowerKey, Key upperKey) {
				this.transaction = transaction;
				stack = new TreeSystemStack(transaction);
				lower_key = PreviousKeyOrder(lowerKey);
				this.upper_key = upperKey;
				p = 0;

				version = -1;
				start = -1;
				end = -1;
			}

			internal ITreeSystem TreeSystem {
				get { return transaction.storeSystem; }
			}

			private TreeSystemTransaction Transaction {
				get { return transaction; }
			}

			private void EnsureCorrectBounds() {
				if (transaction.updateVersion > version) {
					// If version is -1, we force a key position lookup. Version is -1
					// when the range is created or it undergoes a large structural change.
					if (version == -1) {
						// Calculate absolute upper bound,
						end = transaction.AbsKeyEndPosition(upper_key);
						// Calculate the lower bound,
						start = transaction.AbsKeyEndPosition(lower_key);
					} else {
						if (upper_key.CompareTo(transaction.lowestSizeChangedKey) >= 0) {
							// Calculate absolute upper bound,
							end = transaction.AbsKeyEndPosition(upper_key);
						}
						if (lower_key.CompareTo(transaction.lowestSizeChangedKey) > 0) {
							// Calculate the lower bound,
							start = transaction.AbsKeyEndPosition(lower_key);
						}
					}
					// Reset the stack and set the version to the most recent
					stack.Reset();
					version = transaction.updateVersion;
				}
			}

			private void CheckAccessSize(int len) {
				if (p < 0 || p > (end - start - len)) {
					throw new IndexOutOfRangeException("Position out of bounds");
				}
			}

			private void InitWrite() {
				// Generate exception if the backed transaction is read-only.
				if (transaction.readOnly) {
					throw new ApplicationException("Read only transaction.");
				}

				// On writing, we update the versions
				if (version >= 0) {
					++version;
				}
				++transaction.updateVersion;
			}

			// -----

			public long Count {
				get {
					transaction.CheckErrorState();
					try {

						EnsureCorrectBounds();
						return end - start;

					} catch (IOException e) {
						throw transaction.SetErrorState(e);
					} catch (OutOfMemoryException e) {
						throw transaction.SetErrorState(e);
					}
				}
			}

			public long Position {
				get { return p; }
				set { p = value; }
			}

			public Key CurrentKey {
				get {
					transaction.CheckErrorState();
					try {
						EnsureCorrectBounds();
						CheckAccessSize(1);

						stack.SetupForPosition(Key.Tail, start + p);
						return stack.CurrentLeafKey;

					} catch (IOException e) {
						throw transaction.SetErrorState(e);
					} catch (OutOfMemoryException e) {
						throw transaction.SetErrorState(e);
					}
				}
			}

			public long MoveToStart() {
				transaction.CheckErrorState();
				try {
					EnsureCorrectBounds();
					CheckAccessSize(1);

					stack.SetupForPosition(Key.Tail, start + p);
					Key cur_key = stack.CurrentLeafKey;
					long start_of_cur = transaction.AbsKeyEndPosition(PreviousKeyOrder(cur_key)) - start;
					p = start_of_cur;
					return p;

				} catch (IOException e) {
					throw transaction.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}

			public long MoveNext() {
				transaction.CheckErrorState();
				try {

					EnsureCorrectBounds();
					CheckAccessSize(1);

					stack.SetupForPosition(Key.Tail, start + p);
					Key cur_key = stack.CurrentLeafKey;
					long start_of_next = transaction.AbsKeyEndPosition(cur_key) - start;
					p = start_of_next;
					return p;

				} catch (IOException e) {
					throw transaction.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}

			public long MovePrevious() {
				transaction.CheckErrorState();
				try {

					EnsureCorrectBounds();
					CheckAccessSize(0);

					// TODO: This seems rather complicated. Any way to simplify?

					// Special case, if we are at the end,
					long start_of_cur;
					if (p == (end - start)) {
						start_of_cur = p;
					}
						//
					else {
						stack.SetupForPosition(Key.Tail, start + p);
						Key cur_key = stack.CurrentLeafKey;
						start_of_cur = transaction.AbsKeyEndPosition(PreviousKeyOrder(cur_key)) - start;
					}
					// If at the start then we can't go to previous,
					if (start_of_cur == 0) {
						throw new IndexOutOfRangeException("On first key");
					}
					// Decrease the pointer and find the key and first position of that
					--start_of_cur;
					stack.SetupForPosition(Key.Tail, start + start_of_cur);
					Key prev_key = stack.CurrentLeafKey;
					long start_of_prev = transaction.AbsKeyEndPosition(PreviousKeyOrder(prev_key)) - start;

					p = start_of_prev;
					return p;

				} catch (IOException e) {
					throw transaction.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}

			public DataFile GetFile(FileAccess access) {
				transaction.CheckErrorState();
				try {

					EnsureCorrectBounds();
					CheckAccessSize(1);

					stack.SetupForPosition(Key.Tail, start + p);
					Key cur_key = stack.CurrentLeafKey;

					return transaction.GetFile(cur_key, access);

				} catch (IOException e) {
					throw transaction.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}

			public DataFile GetFile(Key key, FileAccess access) {
				transaction.CheckErrorState();
				try {

					// Check the key is within range,
					if (key.CompareTo(lower_key) < 0 ||
						key.CompareTo(upper_key) > 0) {
						throw new IndexOutOfRangeException("Key out of bounds");
					}

					return transaction.GetFile(key, access);

				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}

			public void ReplicateTo(IDataRange target) {
				if (target is TransactionDataRange) {
					// If the tree systems are different we fall back
					TransactionDataRange t_target = (TransactionDataRange)target;
					if (TreeSystem == t_target.TreeSystem) {
						// Fail condition (same transaction),
						if (t_target.Transaction == Transaction) {
							throw new ArgumentException("'ReplicateTo' on the same transaction");
						}

						// Ok, different transaction, same tree system source, both
						// TranDataRange objects, so we can do an efficient tree copy.

						// PENDING,


					}
				}

				// The fallback method,
				// This uses the standard API to replicate all the keys in the target
				// range.
				// Note that if the target can't contain the keys because they fall
				//  outside of its bound then the exception comes from the target.
				target.Delete();
				long sz = Count;
				long pos = 0;
				while (pos < sz) {
					Position = pos;
					Key key = CurrentKey;
					DataFile df = GetFile(FileAccess.Read);
					DataFile target_df = target.GetFile(key, FileAccess.ReadWrite);
					df.ReplicateTo(target_df);
					pos = MoveNext();
				}

			}

			public void Delete() {
				transaction.CheckErrorState();
				try {

					InitWrite();
					EnsureCorrectBounds();

					if (end > start) {
						// Remove the data,
						transaction.RemoveAbsoluteBounds(start, end);
					}
					if (end < start) {
						// Should ever happen?
						throw new ApplicationException("end < start");
					}

					transaction.FlushCache();

				} catch (IOException e) {
					throw transaction.SetErrorState(e);
				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}
		}

		#endregion

		#region KeyCollection

		private class KeyCollection : ICollection<Key> {
			private readonly TreeSystemTransaction transaction;
			private readonly IDataRange range;
			private bool iterating;

			public KeyCollection(TreeSystemTransaction transaction, IDataRange range) {
				this.transaction = transaction;
				this.range = range;
			}

			#region Implementation of IEnumerable

			public IEnumerator<Key> GetEnumerator() {
				if (iterating)
					throw new InvalidOperationException();

				transaction.CheckErrorState();

				try {
					return new KeyEnumerator(transaction, this);
				} catch (OutOfMemoryException e) {
					throw transaction.SetErrorState(e);
				}
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			#endregion

			#region Implementation of ICollection<Key>

			public void Add(Key item) {
				throw new NotSupportedException();
			}

			public void Clear() {
				throw new NotSupportedException();
			}

			public bool Contains(Key item) {
				if (iterating)
					throw new InvalidOperationException();

				foreach(Key key in this) {
					if (key.Equals(item))
						return true;
				}

				return false;
			}

			public void CopyTo(Key[] array, int arrayIndex) {
				if (iterating)
					throw new InvalidOperationException();

				IEnumerator<Key> en = GetEnumerator();
				for (int i = arrayIndex; i < array.Length && en.MoveNext(); i++) {
					array[i] = en.Current;
				}
			}

			public bool Remove(Key item) {
				throw new NotSupportedException();
			}

			public int Count {
				get { return (int) range.Count; }
			}

			public bool IsReadOnly {
				get { return true; }
			}

			#endregion

			private void OnIterationEnded() {
				iterating = false;
			}

			#region KeyEnumerator

			private class KeyEnumerator : IEnumerator<Key> {
				private Key key;
				private readonly TreeSystemTransaction tran;
				private readonly KeyCollection collection;

				internal KeyEnumerator(TreeSystemTransaction tran, KeyCollection collection) {
					this.tran = tran;
					this.collection = collection;
				}

				#region IEnumerator<Key> Members

				public bool MoveNext() {
					bool hasNext = collection.range.Position < collection.range.Count;
					if (hasNext) {
						key = collection.range.CurrentKey;
						collection.range.MoveNext();
					}
					return hasNext;
				}

				public void Reset() {
					collection.range.MoveToStart();
				}

				object IEnumerator.Current {
					get { return Current; }
				}

				public Key Current {
					get { return key; }
				}

				#endregion

				#region Implementation of IDisposable

				void IDisposable.Dispose() {
				}

				#endregion
			}

			#endregion

		}

		#endregion
	}
}