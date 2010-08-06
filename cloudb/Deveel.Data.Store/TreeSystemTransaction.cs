using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Store {
	internal class TreeSystemTransaction : ITransaction {
		private long rootNodeId;
		private readonly long versionId;
		private List<long> nodeDeletes;
		private List<long> nodeInserts;
		private TreeNodeHeap nodeHeap;
		private readonly ITreeStorageSystem storeSystem;
		private long updateVersion;
		private Key lowestSizeChangedKey = Key.Tail;
		private int treeHeight = -1;

		private bool readOnly;
		private bool disposed;
		private bool committed;
		private bool non_committable;

		internal TreeSystemTransaction(ITreeStorageSystem storeSystem, long versionId, long rootNodeId, bool readOnly) {
			this.storeSystem = storeSystem;
			this.rootNodeId = rootNodeId;
			this.versionId = versionId;
			updateVersion = 0;
			nodeDeletes = null;
			nodeInserts = null;
			this.readOnly = readOnly;
			disposed = false;

		}

		public long RootNodeId {
			get { return rootNodeId; }
		}

		public TreeNodeHeap NodeHeap {
			get {
				if (nodeHeap == null)
					nodeHeap = new TreeNodeHeap(13999, storeSystem.NodeHeapMaxSize);
				return nodeHeap;
			}
		}

		private List<long> NodeDeletes {
			get {
				if (nodeDeletes == null)
					nodeDeletes = new List<long>(64);
				return nodeDeletes;
			}
		}

		private List<long> NodeInserts {
			get {
				if (nodeInserts == null)
					nodeInserts = new List<long>(64);
				return nodeInserts;
			}
		}

		internal IList<long> DeletedNodes {
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


		private TreeLeaf CreateSparseLeaf(Key key, byte b, long max_size) {
			// Make sure the sparse leaf doesn't exceed the maximum leaf size
			int sparse_size = (int)Math.Min(max_size, (long)MaxLeafByteSize);
			// Make sure the sparse leaf doesn't exceed the maximum size of the
			// sparse leaf object.
			sparse_size = Math.Min(65535, sparse_size);

			// The byte encoding
			int byte_code = (((int)b) & 0x0FF) << 16;
			// Merge all the info into the sparse node reference
			int sparse_code = sparse_size | byte_code | 0x01000000;
			long node_ref = 0x01000000000000000L + sparse_code;

			return (TreeLeaf)FetchNode(node_ref);
		}

		private TreeLeaf CreateLeaf(Key key) {
			return NodeHeap.CreateLeaf(this, key, MaxLeafByteSize);
		}

		private TreeBranch CreateBranch() {
			return NodeHeap.CreateBranch(this, MaxBranchSize);
		}

		private static bool IsFrozen(long nodeId) {
			// A node is frozen if either it is in the store (nodeId >= 0) or it has
			// the lock bit set to 0
			return nodeId >= 0 || (nodeId & 0x02000000000000000L) == 0;
		}

		private ITreeNode UnfreezeNode(ITreeNode node) {
			long node_ref = node.Id;
			if (IsFrozen(node_ref)) {
				// Return a copy of the node
				ITreeNode new_copy = NodeHeap.Copy(node, storeSystem.MaxBranchSize,
				                                   storeSystem.MaxLeafByteSize, this, false);
				// Delete the old node,
				DeleteNode(node_ref);
				return new_copy;
			}
			return node;
		}

		private ITreeNode FetchNode(long nodeId) {
			// Is it a node we can fetch from the local node heap?
			if (IsHeapNode(nodeId)) {
				ITreeNode n = NodeHeap.FetchNode(nodeId);
				if (n == null)
					throw new NullReferenceException();
				return n;
			}
			// Otherwise fetch the node from the tree store
			return storeSystem.FetchNodes<ITreeNode>(new long[] { nodeId })[0];
		}

		private static bool IsHeapNode(long nodeId) {
			return nodeId < 0;
		}

		private static void ByteBufferCopyTo(DataFile source, DataFile target, long size) {
			long pos = target.Position;
			// Make room to insert the data
			target.Shift(size);
			target.Position = pos;
			// Set a 1k buffer
			byte [] buf = new byte[1024];
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

		private int PopulateWrite(long reference, TreeWrite write) {
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
					long child_ref = branch.GetChild(i);
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

		private long WriteNode(long reference) {
			// Create the sequence,
			TreeWrite treeWrite = new TreeWrite();
			// Create the command sequence to write this tree out,
			int root_id = PopulateWrite(reference, treeWrite);

			if (root_id != -1) {
				// Write out this sequence,
				IList<long> refs = storeSystem.Persist(treeWrite);

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

		private void OnNodeWritten(ITreeNode node, long reference) {
			// Delete the reference to the old node,
			DeleteNode(node.Id);
			// Log the insert operation.
			LogStoreChange(1, reference);
		}

		private void DeleteNode(long pointer) {
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

		private void LogStoreChange(byte type, long pointer) {
			if ((pointer & 0x02000000000000000L) != 0)
				// This could happen if there's a pointer overflow
				throw new ApplicationException("Pointer error.");
			// Special node type changes are not logged
			if ((pointer & 0x01000000000000000L) != 0)
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

		private long KeyStartPosition(Key left_key, Key key, long relative_offset, long reference, long node_total_size, int cur_height) {
			// If we know this is a leaf,
			if (cur_height == treeHeight) {
				// If the key matches,
				int c = key.CompareTo(left_key);
				if (c == 0) {
					return relative_offset;
				}
					// If the searched for key is less than this
				if (c < 0)
					return -(relative_offset + 1);
					// If this key is greater, relative offset is at the end of this node.
				if (c > 0)
					return -((relative_offset + node_total_size) + 1);
			
				// Shouldn't be possible to get here!
				throw new SystemException();
			}

			// Fetch the node
			ITreeNode node = FetchNode(reference);
			// If the node is a branch node
			if (node is TreeBranch) {
				TreeBranch branch = (TreeBranch)node;
				// We ask the node for the child sub-tree that will contain this node
				int child_i = branch.SearchFirst(key);
				if (child_i >= 0) {
					// Child will be in this subtree
					long child_offset = branch.GetChildOffset(child_i);
					long child_ref = branch.GetChild(child_i);
					long total_size = branch.GetChildLeafElementCount(child_i);
					// Set up the left key
					Key new_left_key = (child_i > 0) ? branch.GetKey(child_i) : left_key;
					// Recurse,
					return KeyStartPosition(new_left_key, key, relative_offset + child_offset, child_ref, total_size, cur_height + 1);
				} else {
					// A negative child_i means that the key_ref is equal to the key being
					// searched, so we must walk the left child then the right child to
					// find the position.
					child_i = -(child_i + 1);
					// We must search left first because that is where the first occurance
					// we be.
					long child_offset = branch.GetChildOffset(child_i);
					long child_ref = branch.GetChild(child_i);
					long total_size = branch.GetChildLeafElementCount(child_i);
					// Set up the left key
					Key new_left_key = (child_i > 0) ? branch.GetKey(child_i) : left_key;
					// Recurse,
					long pos = KeyStartPosition(new_left_key, key,
												relative_offset + child_offset, child_ref,
												total_size, cur_height + 1);
					// If we didn't find first down the left, then try the right
					if (pos < 0) {
						// Increment child_i
						++child_i;
						child_offset = branch.GetChildOffset(child_i);
						child_ref = branch.GetChild(child_i);
						total_size = branch.GetChildLeafElementCount(child_i);
						pos = KeyStartPosition(branch.GetKey(child_i), key, relative_offset + child_offset, child_ref, total_size, cur_height + 1);
					}
					if (pos < 0) {
						// Assertion failed, we didn't find it down the right route either!
						//  This means the B+tree has lost integrity.
						throw new ApplicationException("Assertion failed: " +
										 "value not found in left or right branch of tree.");
					}
					// Return the reference position
					return pos;
				}
			} else {
				// If the node is a leaf,
				TreeLeaf leaf = (TreeLeaf)node;

				// Assertion
				if (leaf.Length != node_total_size) {
					throw new SystemException();
				}

				// Set the tree_height var
				treeHeight = cur_height;

				// If the key matches,
				int c = key.CompareTo(left_key);
				if (c == 0)
					return relative_offset;
					// If the searched for key is less than this 
				if (c < 0)
					return -(relative_offset + 1);
					// If this key is greater, relative offset is at the end of this node.
				if (c > 0)
					return -((relative_offset + leaf.Length) + 1);

				// Shouldn't be possible to get here!
				throw new SystemException();
			}
		}

		private long KeyStartPosition(Key key) {
			return KeyStartPosition(Key.Head, key, 0, rootNodeId, -1, 1);
		}

		private long KeyEndPosition(Key key) {
			Key left_key = Key.Head;
			int cur_height = 1;
			long left_offset = 0;
			long node_total_size = -1;
			ITreeNode node = FetchNode(rootNodeId);

			while (true) {
				// Is the node a leaf?
				if (node is TreeLeaf) {
					treeHeight = cur_height;
					break;
				}

				// Must be a branch,
				TreeBranch branch = (TreeBranch) node;
				// We ask the node for the child sub-tree that will contain this node
				int child_i = branch.SearchLast(key);
				// Child will be in this subtree
				long child_offset = branch.GetChildOffset(child_i);
				long child_node_ref = branch.GetChild(child_i);
				node_total_size = branch.GetChildLeafElementCount(child_i);
				// Get the left key of the branch if we can
				if (child_i > 0) {
					left_key = branch.GetKey(child_i);
				}
				// Update left_offset
				left_offset += child_offset;

				// Ok, if we know child_node_ref is a leaf,
				if (cur_height + 1 == treeHeight) {
					break;
				}

				// Otherwise, descend to the child and repeat
				node = FetchNode(child_node_ref);
				++cur_height;
			}

			// Ok, we've reached the end of the tree,
			// 'left_key' will be the key of the node we are on,
			// 'node_total_size' will be the size of the node,

			// If the key matches,
			int c = key.CompareTo(left_key);
			if (c == 0)
				return left_offset + node_total_size;
			// If the searched for key is less than this
			if (c < 0)
				return -(left_offset + 1);
			// If this key is greater, relative offset is at the end of this node.
			if (c > 0)
				return -((left_offset + node_total_size) + 1);
			
			throw new SystemException();
		}

		private DataFile UnsafeGetDataFile(Key key, FileAccess access) {
			if (disposed)
				throw new ApplicationException("Transaction is disposed");

			return new TransactionDataFile(this, key, access);
		}

		private void CheckErrorState() {
			storeSystem.CheckErrorState();
		}

		private Exception SetErrorState(Exception error) {
			return storeSystem.SetErrorState(error);
		}

		private void DisposeNode(long node_id) {
			storeSystem.DisposeNode(node_id);
		}

		private void DisposeHeapNodes(long reference) {
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

		private void CompactNodeKey(Key key) {
			long[] merge_buffer = new long[6];
			CompactNode(Key.Head, rootNodeId, merge_buffer, key, key);
		}

		private void CompactNode(Key far_left, long reference, long[] merge_buffer, Key min_bound, Key max_bound) {
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
				int first_child_i = branch.SearchFirst(min_bound);
				int last_child_i = branch.SearchLast(max_bound);

				// first_child_i may be negative which means a key reference is equal
				// to the key being searched, in which case we follow the left branch.
				if (first_child_i < 0)
					first_child_i = -(first_child_i + 1);

				// Compact the children,
				for (int i = first_child_i; i <= last_child_i; ++i) {
					// Change far left to represent the new far left node
					Key new_far_left = (i > 0) ? branch.GetKey(i) : far_left;

					// We don't change max_bound because it's not necessary.
					CompactNode(new_far_left, branch.GetChild(i), merge_buffer, min_bound, max_bound);
				}

				// The number of children in this branch,
				int sz = branch.ChildCount;

				// Now try and merge the compacted children,
				int i1 = first_child_i;
				// We must not let there be less than 3 children
				while (sz > 3 && i1 <= last_child_i - 1) {
					// The left and right children nodes,
					long left_child_ref = branch.GetChild(i1);
					long right_child_ref = branch.GetChild(i1 + 1);

					// If at least one of them is a heap node we attempt to merge the
					// nodes,
					if (IsHeapNode(left_child_ref) || IsHeapNode(right_child_ref)) {
						// Set the left left key and right left key of the references,
						Key left_left_key = (i1 > 0) ? branch.GetKey(i1) : far_left;
						Key right_left_key = branch.GetKey(i1 + 1);
						// Attempt to merge the nodes,
						int node_result = MergeNodes(branch.GetKey(i1 + 1), left_child_ref, right_child_ref, left_left_key, right_left_key,
						                             merge_buffer);
						// If we merged into a single node then we update the left left and
						// delete the right
						if (node_result == 1) {
							branch.SetChild(i1, merge_buffer[0]);
							branch.SetChildLeafElementCount(i1, merge_buffer[1]);
							branch.RemoveChild(i1 + 1);
							// Reduce the size but don't increase i, because we may want to
							// merge again.
							--sz;
							--last_child_i;
						} else if (node_result == 2) {
							// Two result but there was a change (the left was increased in
							// size)
							branch.SetChild(i1, merge_buffer[0]);
							branch.SetChildLeafElementCount(i1, merge_buffer[1]);
							branch.SetKeyValueToLeft(i1 + 1, merge_buffer[2], merge_buffer[3]);
							branch.SetChild(i1 + 1, merge_buffer[4]);
							branch.SetChildLeafElementCount(i1 + 1, merge_buffer[5]);
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

		private int MergeNodes(Key middle_key_value, long left_ref, long right_ref, Key left_left_key, Key right_left_key, long[] merge_buffer) {
			// Fetch the nodes,
			ITreeNode left_node = FetchNode(left_ref);
			ITreeNode right_node = FetchNode(right_ref);
			// Are we merging branches or leafs?
			if (left_node is TreeLeaf) {
				TreeLeaf lleaf = (TreeLeaf)left_node;
				TreeLeaf rleaf = (TreeLeaf)right_node;
				// Check the keys are identical,
				//      if (lleaf.getKey().equals(rleaf.getKey())) {
				if (left_left_key.Equals(right_left_key)) {
					int capacity80 = (int)(0.80 * MaxLeafByteSize);
					// Only proceed if left is less than 80% full,
					if (lleaf.Length < capacity80) {
						// Move elements from the right leaf to the left leaf so that either
						// the right node becomes completely empty or if that's not possible
						// the left node is 80% full.
						if (lleaf.Length + rleaf.Length <= MaxLeafByteSize) {
							// We can fit both nodes into a single node so merge into a single
							// node,
							TreeLeaf nleaf = (TreeLeaf)UnfreezeNode(lleaf);
							byte[] copy_buf = new byte[rleaf.Length];
							rleaf.Read(0, copy_buf, 0, copy_buf.Length);
							nleaf.Write(nleaf.Length, copy_buf, 0, copy_buf.Length);

							// Delete the right node,
							DeleteNode(rleaf.Id);

							// Setup the merge state
							merge_buffer[0] = nleaf.Id;
							merge_buffer[1] = nleaf.Length;
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
								byte[] copy_buf = new byte[to_copy];
								mrleaf.Read(0, copy_buf, 0, to_copy);
								mlleaf.Read(mlleaf.Length, copy_buf, 0, to_copy);
								// Shift the data in the right leaf,
								mrleaf.Shift(to_copy, -to_copy);

								// Return the merge state
								merge_buffer[0] = mlleaf.Id;
								merge_buffer[1] = mlleaf.Length;
								merge_buffer[2] = right_left_key.GetEncoded(1);
								merge_buffer[3] = right_left_key.GetEncoded(2);
								merge_buffer[4] = mrleaf.Id;
								merge_buffer[5] = mrleaf.Length;
								return 2;
							}
						}
					}
				} // leaf keys unequal
			} else if (left_node is TreeBranch) {
				// Merge branches,
				TreeBranch lbranch = (TreeBranch)left_node;
				TreeBranch rbranch = (TreeBranch)right_node;

				int capacity75 = (int)(0.75 * MaxBranchSize);
				// Only proceed if left is less than 75% full,
				if (lbranch.ChildCount < capacity75) {
					// Move elements from the right branch to the left leaf only if the
					// branches can be completely merged into a node
					if (lbranch.ChildCount + rbranch.ChildCount <= MaxBranchSize) {
						// We can fit both nodes into a single node so merge into a single
						// node,
						TreeBranch nbranch = (TreeBranch)UnfreezeNode(lbranch);
						// Merge,
						nbranch.MergeLeft(rbranch, middle_key_value, rbranch.ChildCount);

						// Delete the right branch,
						DeleteNode(rbranch.Id);

						// Setup the merge state
						merge_buffer[0] = nbranch.Id;
						merge_buffer[1] = nbranch.LeafElementCount;
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
							Key new_middle_value = mlbranch.MergeLeft(mrbranch, middle_key_value, to_copy);

							// Setup and return the merge state
							merge_buffer[0] = mlbranch.Id;
							merge_buffer[1] = mlbranch.LeafElementCount;
							merge_buffer[2] = new_middle_value.GetEncoded(1);
							merge_buffer[3] = new_middle_value.GetEncoded(2);
							merge_buffer[4] = mrbranch.Id;
							merge_buffer[5] = mrbranch.LeafElementCount;
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

		private long FlushNodes(long reference, long[] include_refs) {
			if (!IsHeapNode(reference))
				return reference;

			// Is this reference in the list?
			int c = Array.BinarySearch(include_refs, reference);
			if (c < 0) {
				// It was not found, so go to the children,
				// Note that this node will change if it's a branch node, but the
				// reference to it will not change.

				// It is a heap node, so fetch
				ITreeNode node = FetchNode(reference);
				// Is it a leaf or a branch?
				if (node is TreeLeaf)
					return reference;
				if (node is TreeBranch) {
					// This is a branch, so we need to write out any children that are on
					// the heap before we write out the branch itself,
					TreeBranch branch = (TreeBranch) node;

					int sz = branch.ChildCount;

					for (int i = 0; i < sz; ++i) {
						long old_ref = branch.GetChild(i);
						// Recurse
						branch.SetChild(i, FlushNodes(old_ref, include_refs));
					}
					// And return the reference
					return reference;
				}

				throw new Exception("Unknown node type.");
			}

			// This node was in the 'include_refs' list so write it out now,
			return WriteNode(reference);
		}

		private void FlushCache() {
			// When this is called, there should be no locks on anything related to
			// this object.
			NodeHeap.Flush();
		}

		private Key NextKey(Key right_key, Key key, long reference) {
			// Fetch the node
			ITreeNode node = FetchNode(reference);
			// If the node is a branch node
			if (node is TreeBranch) {
				TreeBranch branch = (TreeBranch) node;
				// We ask the node for the child sub-tree that will contain this node
				int child_i = branch.SearchLast(key);
				// Child will be in this subtree
				long child_ref = branch.GetChild(child_i);
				// Get the right key of the branch if we can
				Key new_right_key;
				if (child_i + 1 < branch.ChildCount) {
					new_right_key = branch.GetKey(child_i + 1);
				} else {
					new_right_key = right_key;
				}
				// Recurse,
				return NextKey(new_right_key, key, child_ref);
			}

			// We hit a leaf node, therefore return the 'right_key' value
			return right_key;
		}
		#region Implementation of ITransaction

		public DataFile GetFile(Key key, FileAccess access) {
			CheckErrorState();
			try {
				if (key.Type >= Key.SpecialKeyType)
					throw new ApplicationException("Key is reserved for system data.");

				return UnsafeGetDataFile(key, access);
			} catch (Exception e) {
				throw SetErrorState(e);
			}
		}

		#endregion
		protected void SetToEmpty() {
			// Write a root node to the store,
			try {
				// Create an empty head node
				TreeLeaf head_leaf = CreateLeaf(Key.Head);
				// Insert a tree identification pattern
				head_leaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
				// Create an empty tail node
				TreeLeaf tail_leaf = CreateLeaf(Key.Tail);
				// Insert a tree identification pattern
				tail_leaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

				// Create a branch,
				TreeBranch root_branch = CreateBranch();
				root_branch.Set(head_leaf.Id, 4, Key.Tail.GetEncoded(1), Key.Tail.GetEncoded(2), tail_leaf.Id, 4);

				rootNodeId = root_branch.Id;

			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			// If it's not already disposed,
			if (!disposed) {
				// Walk the tree and dispose all nodes on the heap,
				DisposeHeapNodes(rootNodeId);
				if (!committed) {
					// Then dispose all nodes that were inserted during the operation of
					// this transaction
					if (nodeInserts != null) {
						int sz = NodeInserts.Count;
						for (int i = 0; i < sz; ++i) {
							long node_id = NodeInserts[i];
							DisposeNode(node_id);
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

		internal void FlushNodes(long[] nids) {
			// If not disposed,
			if (!disposed) {
				// Compact the entire tree
				long[] merge_buffer = new long[6];
				CompactNode(Key.Head, rootNodeId, merge_buffer, Key.Head, Key.Tail);

				// Flush the reference node list,
				rootNodeId = FlushNodes(rootNodeId, nids);

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
			if (rootNodeId < 0)
				throw new ApplicationException("Assertion failed, tree on heap.");
			committed = true;
			readOnly = true;
		}

		public virtual void CheckOut() {
			// Compact the entire tree,
			long[] merge_buffer = new long[6];
			CompactNode(Key.Head, rootNodeId, merge_buffer, Key.Head, Key.Tail);
			// Write out the changes
			rootNodeId = WriteNode(rootNodeId);

			// Update the version so any data file objects will flush with the
			// changes.
			++updateVersion;

			FlushCache();
		}

		public long FastSizeCalculate() {
			ITreeNode node = FetchNode(rootNodeId);
			if (node is TreeBranch) {
				TreeBranch branch = (TreeBranch) node;
				int sz = branch.ChildCount;
				// Add up the sizes of the children in the branch
				long r_size = 0;
				for (int i = 0; i < sz; ++i) {
					r_size += branch.GetChildLeafElementCount(i);
				}
				return r_size;
			}

			TreeLeaf leaf = (TreeLeaf) node;
			return leaf.Length;
		}

		public IEnumerator<Key> GetKeyEnumerator() {
			CheckErrorState();

			try {
				return new KeyEnumerator(this);
			} catch (OutOfMemoryException e) {
				throw SetErrorState(e);
			}
		}

		#region TreeStack

		private class TreeStack {
			private readonly TreeSystemTransaction tnx;
			private int stack_size;
			private long[] stack;
			private TreeLeaf current_leaf;
			private Key current_leaf_key;
			private int leaf_offset;

			public TreeStack(TreeSystemTransaction tnx) {
				this.tnx = tnx;
				stack_size = 0;
				stack = new long[3 * 13];
				current_leaf = null;
				current_leaf_key = null;
				leaf_offset = 0;
			}

			private void Push(int child_i, long offset, long node_pointer) {
				if (stack_size + 3 >= stack.Length) {
					// Expand the size of the stack.
					// The default size should be plenty for most iterators unless we
					// happen to be iterating across a particularly deep B+Tree.
					long[] new_stack = new long[stack.Length * 2];
					Array.Copy(stack, 0, new_stack, 0, stack.Length);
					stack = new_stack;
				}
				stack[stack_size] = child_i;
				stack[stack_size + 1] = offset;
				stack[stack_size + 2] = node_pointer;
				stack_size += 3;
			}

			private long Pop() {
				if (stack_size == 0)
					throw new ApplicationException("Iterator stack underflow.");
				--stack_size;
				long v = stack[stack_size];
				return v;
			}

			private long End(int off) {
				return stack[stack_size - off - 1];
			}

			private bool IsEmpty {
				get { return (stack_size == 0); }
			}

			private void Clear() {
				stack_size = 0;
			}

			private void Unfreeze() {
				long old_child_node_ref = stack[stack_size - 1];
				// If the leaf reference isn't frozen then we exit early
				if (!IsFrozen(stack[stack_size - 1])) {
					return;
				}
				TreeLeaf leaf = (TreeLeaf)tnx.UnfreezeNode(tnx.FetchNode(old_child_node_ref));
				long new_child_node_ref = leaf.Id;
				stack[stack_size - 1] = new_child_node_ref;
				current_leaf = leaf;
				// NOTE: Setting current_leaf here does not change the key of the node
				//   so we don't need to update current_leaf_key.

				// Walk the stack from the end
				for (int i = stack_size - 4; i >= 1; i -= 3) {
					long old_branch_ref = stack[i];
					TreeBranch branch =
								   (TreeBranch)tnx.UnfreezeNode(tnx.FetchNode(old_branch_ref));
					// Get the child_i from the stack,
					int changed_child_i = (int)stack[i + 1];
					branch.SetChild(changed_child_i, new_child_node_ref);

					// Change the stack entry
					stack[i] = branch.Id;

					//        old_child_node_ref = old_branch_ref;
					new_child_node_ref = branch.Id;
				}

				// Set the new root node reference
				tnx.rootNodeId = stack[2];
			}

			internal void WriteLeafOnly(Key key) {
				long leaf_ref = stack[stack_size - 1];
				long new_ref = tnx.WriteNode(leaf_ref);

				if (new_ref == leaf_ref)
					return;

				// Otherwise, update the references,
				stack[stack_size - 1] = new_ref;
				current_leaf = (TreeLeaf) tnx.FetchNode(new_ref);
				// Walk back up the stack and update the reference as necessary
				for (int i = stack_size - 4; i >= 1; i -= 3) {
					long old_branch_ref = stack[i];
					TreeBranch branch = (TreeBranch) tnx.UnfreezeNode(tnx.FetchNode(old_branch_ref));
					// Get the child_i from the stack,
					int changed_child_i = (int) stack[i + 1];
					branch.SetChild(changed_child_i, new_ref);

					// Change the stack entry
					stack[i] = branch.Id;

					new_ref = branch.Id;
				}

				// Set the new root node reference
				tnx.rootNodeId = stack[2];
			}

			private void updateStackProperties(int size_diff) {
				// Walk the stack from the end
				for (int i = stack_size - 4; i >= 1; i -= 3) {
					TreeBranch branch = (TreeBranch)tnx.FetchNode(stack[i]);
					//int child_i = branch.childWithReference(node_ref);
					int child_i = (int)stack[i + 1];
					branch.SetChildLeafElementCount(child_i, branch.GetChildLeafElementCount(child_i) + size_diff);
				}
			}

			internal void InsertLeaf(Key new_leaf_key, TreeLeaf new_leaf, bool before) {
				int leaf_size = new_leaf.Length;
				if (leaf_size <= 0)
					throw new ArgumentException("size <= 0");

				// The current absolute position and key
				//      final long cur_absolute_pos = stack[stack_size - 2] + leaf_offset;
				Key new_key = new_leaf_key;

				TreeLeaf left, right;
				long key_ref;

				long current_leaf_ref = stack[stack_size - 1];

				long[] nfo;
				long[] r_nfo = new long[6];
				Key left_key;
				long cur_absolute_pos;
				// If we are inserting the new leaf after,
				if (!before) {
					Key k = new_leaf_key;
					nfo = new long[] { current_leaf.Id, current_leaf.Length, k.GetEncoded(1), k.GetEncoded(2), new_leaf.Id, new_leaf.Length};
					left_key = null;
					cur_absolute_pos = stack[stack_size - 2] + current_leaf.Length;
				}
					// Otherwise we are inserting the new leaf before,
				else {
					// If before and current_leaf key is different than new_leaf key, we
					// generate an error
					if (!current_leaf_key.Equals(new_leaf_key)) {
						throw new ApplicationException("Can't insert different new key before.");
					}
					Key k = current_leaf_key;
					nfo = new long[] { new_leaf.Id, new_leaf.Length, k.GetEncoded(1), k.GetEncoded(2), current_leaf.Id, current_leaf.Length};
					left_key = new_leaf_key;
					cur_absolute_pos = stack[stack_size - 2] - 1;
				}

				bool insert_two_nodes = true;
				for (int i = stack_size - 4; i >= 0; i -= 3) {
					// The child reference of this stack element
					long child_ref = stack[i];
					// Fetch it
					TreeBranch branch = (TreeBranch)tnx.UnfreezeNode(tnx.FetchNode(child_ref));
					//        long branch_ref = branch.getReference();
					int child_i = (int)stack[i + 1];
					//        int child_i = branch.childWithReference(child_ref);

					// Do we have two nodes to insert into the branch?
					if (insert_two_nodes) {
						TreeBranch insert_branch;
						int insert_n = child_i;
						// If the branch is full,
						if (branch.IsFull) {
							// Create a new node,
							TreeBranch left_branch = branch;
							TreeBranch right_branch = tnx.CreateBranch();
							// Split the branch,
							Key midpoint_key = left_branch.MidPointKey;
							// And move half of this branch into the new branch
							left_branch.MoveLastHalfInto(right_branch);
							// We split so we need to return a split flag,
							r_nfo[0] = left_branch.Id;
							r_nfo[1] = left_branch.LeafElementCount;
							r_nfo[2] = midpoint_key.GetEncoded(1);
							r_nfo[3] = midpoint_key.GetEncoded(2);
							r_nfo[4] = right_branch.Id;
							r_nfo[5] = right_branch.LeafElementCount;
							// Adjust insert_n and insert_branch
							if (insert_n >= left_branch.ChildCount) {
								insert_n -= left_branch.ChildCount;
								insert_branch = right_branch;
								r_nfo[5] += new_leaf.Length;
								// If insert_n == 0, we change the midpoint value to the left
								// key value,
								if (insert_n == 0 && left_key != null) {
									r_nfo[2] = left_key.GetEncoded(1);
									r_nfo[3] = left_key.GetEncoded(2);
									left_key = null;
								}
							} else {
								insert_branch = left_branch;
								r_nfo[1] += new_leaf.Length;
							}
						}
							// If it's not full,
						else {
							insert_branch = branch;
							r_nfo[0] = insert_branch.Id;
							insert_two_nodes = false;
						}
						// Insert the two children nodes
						insert_branch.Insert(nfo[0], nfo[1], nfo[2], nfo[3], nfo[4], nfo[5], insert_n);
						// Copy r_nfo to nfo
						for (int p = 0; p < r_nfo.Length; ++p) {
							nfo[p] = r_nfo[p];
						}

						// Adjust the left key reference if necessary
						if (left_key != null && insert_n > 0) {
							insert_branch.SetKeyValueToLeft(left_key, insert_n);
							left_key = null;
						}
					} else {
						branch.SetChild(child_i, nfo[0]);
						nfo[0] = branch.Id;
						branch.SetChildLeafElementCount(child_i, branch.GetChildLeafElementCount(child_i) + leaf_size);

						// Adjust the left key reference if necessary
						if (left_key != null && child_i > 0) {
							branch.SetKeyValueToLeft(left_key, child_i);
							left_key = null;
						}
					}

				} // For all elements in the stack,

				// At the end, if we still have a split then we make a new root and
				// adjust the stack accordingly
				if (insert_two_nodes) {
					TreeBranch new_root = tnx.CreateBranch();
					new_root.Set(nfo[0], nfo[1], nfo[2], nfo[3], nfo[4], nfo[5]);
					tnx.rootNodeId = new_root.Id;
					// The tree height has increased,
					if (tnx.treeHeight != -1) {
						++tnx.treeHeight;
					}
				} else {
					tnx.rootNodeId = nfo[0];
				}

				// Now reset the position,
				Reset();
				SetupForPosition(new_key, cur_absolute_pos);
			}

			private void RedistributeBranchElements(TreeBranch branch, int childIndex, TreeBranch child) {
				// We distribute the nodes in the child branch with the branch
				// immediately to the right.  If that's not possible, then we distribute
				// with the left.

				int left_i, right_i;
				TreeBranch left, right;
				if (childIndex < branch.ChildCount - 1) {
					// Distribute with the right
					left_i = childIndex;
					right_i = childIndex + 1;
					left = child;
					right = (TreeBranch)tnx.UnfreezeNode(tnx.FetchNode(branch.GetChild(childIndex + 1)));
					branch.SetChild(childIndex + 1, right.Id);
				} else {
					// Distribute with the left
					left_i = childIndex - 1;
					right_i = childIndex;
					left = (TreeBranch)tnx.UnfreezeNode(tnx.FetchNode(branch.GetChild(childIndex - 1)));
					right = child;
					branch.SetChild(childIndex - 1, left.Id);
				}

				// Get the mid value key reference
				Key mid_key = branch.GetKey(right_i);

				// Perform the merge,
				Key new_mid_key = left.Merge(right, mid_key);
				// Reset the leaf element count
				branch.SetChildLeafElementCount(left_i, left.LeafElementCount);
				branch.SetChildLeafElementCount(right_i, right.LeafElementCount);

				// If after the merge the right branch is empty, we need to remove it
				if (right.IsEmpty) {
					// Delete the node
					tnx.DeleteNode(right.Id);
					// And remove it from the branch,
					branch.RemoveChild(right_i);
				} else {
					// Otherwise set the key reference
					branch.SetKeyValueToLeft(new_mid_key, right_i);
				}
			}


			// ----- Public methods -----

			public TreeLeaf CurrentLeaf {
				get { return current_leaf; }
			}

			public Key CurrentLeafKey {
				get { return current_leaf_key; }
			}

			public int LeafOffset {
				get { return leaf_offset; }
			}

			public bool IsAtEndOfKeyData {
				get { return LeafOffset >= LeafLength; }
			}

			public void SetupForPosition(Key key, long posit) {
				// If the current leaf is set
				if (current_leaf != null) {
					long leaf_start = End(1);
					long leaf_end = leaf_start + current_leaf.Length;
					// If the position is at the leaf end, or if the keys aren't equal, we
					// need to reset the stack.  This ensures that we correctly place the
					// pointer.
					if (posit == leaf_end || !key.Equals(current_leaf_key)) {
						Clear();
						current_leaf = null;
						current_leaf_key = null;
					} else {
						// Check whether the position is within the bounds of the current leaf
						// If 'posit' is within this leaf
						if (posit >= leaf_start && posit < leaf_end) {
							// If the position is within the current leaf, set up the internal
							// vars as necessary.
							leaf_offset = (int)(posit - leaf_start);
							return;
						} else {
							// If it's not, we reset the stack and start fresh,
							Clear();
							current_leaf = null;
							current_leaf_key = null;
						}
					}
				}

				// ISSUE: It appears looking at the code above, the stack will always be
				//   empty and current_leaf will always be null if we get here.

				// If the stack is empty, push the root node,
				if (IsEmpty) {
					// Push the root node onto the top of the stack.
					Push(-1, 0, tnx.rootNodeId);
					// Set up the current_leaf_key to the default value
					current_leaf_key = Key.Head;
				}
				// Otherwise, we need to setup by querying the BTree.
				while (true) {
					if (IsEmpty) {
						throw new ApplicationException("Position out of bounds.  p = " + posit);
					}

					long node_pointer = Pop();
					long left_side_offset = Pop();
					int node_child_i = (int)Pop();
					// Relative offset within this node
					long relative_offset = posit - left_side_offset;

					// If the node is not on the heap,
					if (!IsHeapNode(node_pointer)) {
						// The node is not on the heap. We optimize here.
						// If we know the node is going to be a leaf node, we set up a
						// temporary leaf node object with as much information as we know.

						// Check if we know this is a leaf
						if (tnx.treeHeight != -1) {
							if ((stack_size / 3) + 1 == tnx.treeHeight) {
								// Fetch the parent node,
								long twig_node_pointer = End(0);
								TreeBranch twig = (TreeBranch)tnx.FetchNode(twig_node_pointer);
								long leaf_size = twig.GetChildLeafElementCount(node_child_i);

								// This object holds off fetching the contents of the leaf node
								// unless it's absolutely required.
								TreeLeaf leaf = new PlaceholderLeaf(tnx, node_pointer, (int)leaf_size);

								current_leaf = leaf;
								Push(node_child_i, left_side_offset, node_pointer);
								// Set up the leaf offset and return
								leaf_offset = (int)relative_offset;
								return;
							}
						}
					}

					// Fetch the node
					ITreeNode node = tnx.FetchNode(node_pointer);
					if (node is TreeLeaf) {
						// Node is a leaf node
						TreeLeaf leaf = (TreeLeaf)node;

						current_leaf = leaf;
						Push(node_child_i, left_side_offset, node_pointer);
						// Set up the leaf offset and return
						leaf_offset = (int)relative_offset;

						// Update the treeHeight value,
						tnx.treeHeight = (stack_size / 3);
						return;
					} else {
						// Node is a branch node
						TreeBranch branch = (TreeBranch)node;
						int child_i = branch.IndexOfChild(key, relative_offset);
						if (child_i != -1) {
							// Push the current details,
							Push(node_child_i, left_side_offset, node_pointer);
							// Found child so push the details
							Push(child_i,
									  branch.GetChildOffset(child_i) + left_side_offset,
									  branch.GetChild(child_i));
							// Set up the left key
							if (child_i > 0) {
								current_leaf_key = branch.GetKey(child_i);
							}
						}
					}
				} // while (true)
			}

			public void DeleteLeaf(Key key) {

				// The leaf
				long leaf_ref = stack[stack_size - 1];
				// Delete the leaf,
				tnx.DeleteNode(leaf_ref);

				Key left_key = null;

				// Go to the twig and remove this reference,
				long this_ref = stack[stack_size - 4];
				TreeBranch branch_node = (TreeBranch)tnx.UnfreezeNode(tnx.FetchNode(this_ref));
				// The offset of the child
				int child_i = (int)stack[stack_size - 3];
				// The size of the leaf element being deleted,
				int delete_node_size =
					(int)branch_node.GetChildLeafElementCount(child_i);

				// If this is the first reference,
				if (child_i == 0) {
					left_key = branch_node.GetKey(1);
					branch_node.RemoveChild(0);
				} else {
					branch_node.RemoveChild(child_i);
				}

				// Walk back through the stack
				for (int i = stack_size - 7; i >= 1; i -= 3) {
					this_ref = stack[i];
					TreeBranch child_branch = branch_node;
					branch_node = (TreeBranch)tnx.UnfreezeNode(tnx.FetchNode(this_ref));

					// Find the child_i for the child
					//        child_i = branch_node.childWithReference(child_ref);
					child_i = (int)stack[i + 1];

					// Replace with the new child node reference
					branch_node.SetChild(child_i, child_branch.Id);
					// Set the element count
					long new_child_size = branch_node.GetChildLeafElementCount(child_i) - delete_node_size;
					branch_node.SetChildLeafElementCount(child_i, new_child_size);
					// Can we set the left key reference?
					if (child_i > 0 && left_key != null) {
						branch_node.SetKeyValueToLeft(left_key, child_i);
						left_key = null;
					}

					// Has the size of the child reached the lower threshold?
					if (child_branch.ChildCount <= 2) {
						// If it has, we need to redistribute the children,
						RedistributeBranchElements(branch_node, child_i, child_branch);
					}

				}

				// Finally, set the root node
				// If the branch node is a single element, we set the root as the child,
				if (branch_node.ChildCount == 1) {
					// This shrinks the height of the tree,
					tnx.rootNodeId = branch_node.GetChild(0);
					if (tnx.treeHeight != -1) {
						tnx.treeHeight = tnx.treeHeight - 1;
					}
				} else {
					// Otherwise, we set the branch node.
					tnx.rootNodeId = branch_node.Id;
				}

				// Reset the object
				Reset();

			}

			public void RemoveSpaceBefore(Key key, long amount_to_remove) {
				long p = (stack[stack_size - 2] + leaf_offset) - 1;
				while (true) {
					SetupForPosition(key, p);
					int leaf_size = LeafLength;
					if (amount_to_remove >= leaf_size) {
						DeleteLeaf(key);
						amount_to_remove -= leaf_size;
					} else {
						if (amount_to_remove > 0) {
							SetupForPosition(key, (p - amount_to_remove) + 1);
							TrimAtPosition();
						}
						return;
					}
					p -= leaf_size;
				}
			}

			public long DeleteAllNodesBackTo(Key key, long back_position) {
				long p = stack[stack_size - 2] - 1;
				long bytes_removed = 0;

				while (true) {
					// Set up for the node,
					SetupForPosition(key, p);
					// This is the stopping condition, when the start of the node is
					// before the back_position,
					if (stack[stack_size - 2] <= back_position)
						return bytes_removed;

					// The current leaf size
					int leaf_size = LeafLength;
					// The bytes removed is the size of the leaf,
					bytes_removed += LeafLength;
					// Otherwise, delete the leaf
					DeleteLeaf(key);

					p -= leaf_size;
				}
			}

			public void SplitLeaf(Key key, long position) {
				Unfreeze();
				TreeLeaf source_leaf = CurrentLeaf;
				int split_point = LeafOffset;
				// The amount of data we are copying from the current key.
				int amount = source_leaf.Length - split_point;
				// Create a new empty node
				TreeLeaf empty_leaf = tnx.CreateLeaf(key);
				empty_leaf.SetLength(amount);
				// Copy the data at the end of the leaf into a buffer
				byte[] buf = new byte[amount];
				source_leaf.Read(split_point, buf, 0, amount);
				// And write it out to the new leaf
				empty_leaf.Write(0, buf, 0, amount);
				// Set the new size of the node
				source_leaf.SetLength(split_point);
				// Update the stack properties
				updateStackProperties(-amount);
				// And insert the new leaf after
				InsertLeaf(key, empty_leaf, false);
			}

			public void AddSpaceAfter(Key key, long space_to_add) {
				while (space_to_add > 0) {
					// Create an empty sparse node
					TreeLeaf empty_leaf = tnx.CreateSparseLeaf(key, (byte)0, space_to_add);
					InsertLeaf(key, empty_leaf, false);
					space_to_add -= empty_leaf.Length;
				}
			}

			public int ExpandLeaf(long amount) {
				if (amount > 0) {
					Unfreeze();
					int actual_expand_by = (int)Math.Min((long)current_leaf.Capacity - current_leaf.Length, amount);
					if (actual_expand_by > 0) {
						current_leaf.SetLength(current_leaf.Length + actual_expand_by);
						updateStackProperties(actual_expand_by);
					}
					return actual_expand_by;
				}
				return 0;
			}

			public void TrimAtPosition() {
				Unfreeze();
				int size_before = current_leaf.Length;
				current_leaf.SetLength(leaf_offset);
				updateStackProperties(leaf_offset - size_before);
			}

			public void ShiftLeaf(long amount) {
				if (amount != 0) {
					Unfreeze();
					int size_before = current_leaf.Length;
					current_leaf.Shift(leaf_offset, (int)amount);
					updateStackProperties(current_leaf.Length - size_before);
				}
			}

			public bool MoveToNextLeaf(Key key) {
				long next_pos = stack[stack_size - 2] + current_leaf.Length;
				SetupForPosition(key, next_pos);
				return leaf_offset != current_leaf.Length && current_leaf_key.Equals(key);
			}

			public bool MoveToPreviousLeaf(Key key) {
				long previous_pos = stack[stack_size - 2] - 1;
				SetupForPosition(key, previous_pos);
				return current_leaf_key.Equals(key);
			}

			public int LeafLength {
				get { return current_leaf.Length; }
			}


			public int LeafSpareSpace {
				get { return tnx.storeSystem.MaxLeafByteSize - current_leaf.Length; }
			}

			public void ReadInto(Key key, long current_p,
								 byte[] buf, int off, int len) {
				// While there is information to read into the array,
				while (len > 0) {
					// Set up the stack and internal variables for the given position,
					SetupForPosition(key, current_p);
					// Read as much as we can from the current leaf capped at the leaf size
					// if necessary,
					int to_read = Math.Min(len, current_leaf.Length - leaf_offset);
					if (to_read == 0)
						throw new ApplicationException("Read out of bounds.");
					// Copy the leaf into the array,
					current_leaf.Read(leaf_offset, buf, off, to_read);
					// Modify the pointers
					current_p += to_read;
					off += to_read;
					len -= to_read;
				}
			}

			public void WriteFrom(Key key, long current_p, byte[] buf, int off, int len) {
				// While there is information to read into the array,
				while (len > 0) {
					// Set up the stack and internal variables for the given position,
					SetupForPosition(key, current_p);
					// Unfreeze all the nodes currently on the stack,
					Unfreeze();
					// Read as much as we can from the current leaf capped at the leaf size
					// if necessary,
					int to_write = Math.Min(len, current_leaf.Length - leaf_offset);
					if (to_write == 0)
						throw new ApplicationException("Write out of bounds.");
					// Copy the leaf into the array,
					current_leaf.Read(leaf_offset, buf, off, to_write);
					// Modify the pointers
					current_p += to_write;
					off += to_write;
					len -= to_write;
				}
			}

			internal void ShiftData(Key key, long position, long shift_offset) {
				if (shift_offset == 0) {
					return;
				}
				// Set up for the given position
				SetupForPosition(key, position);
				// If there is no leaf node for this key yet, it's an empty file so we
				// add new data and return.
				if (!CurrentLeafKey.Equals(key)) {
					// If we are expanding, then add the extra space and return
					// We can't shrink an empty file.
					if (shift_offset >= 0) {
						// No, so add empty nodes of the required size to make up the space
						AddSpaceAfter(key, shift_offset);
					}
					return;
				}
				// If we are at the end of the data, we simply expand or reduce the data
				// by the shift amount
				if (IsAtEndOfKeyData) {
					if (shift_offset > 0) {
						// Expand,
						long to_expand_by = shift_offset;
						to_expand_by -= ExpandLeaf(to_expand_by);
						// And add nodes for the remaining
						AddSpaceAfter(key, to_expand_by);
						// And return
						return;
					} else {
						// Reduction,
						// Remove the space immediately before this node up to the given
						// amount.
						RemoveSpaceBefore(key, -shift_offset);
						// And return
						return;
					}
				} else {
					// Can we shift data in the leaf and complete the operation?
					if ((shift_offset > 0 && LeafSpareSpace >= shift_offset) ||
						(shift_offset < 0 && LeafOffset + shift_offset >= 0)) {
						// We can simply shift the data in the node
						ShiftLeaf(shift_offset);
						return;
					}
					// There isn't enough space in the current node,
					if (shift_offset > 0) {
						// If we are expanding,
						// The data to copy from the leaf
						int buf_size = LeafLength - LeafOffset;
						byte[] buf = new byte[buf_size];
						ReadInto(key, position, buf, 0, buf_size);
						long leaf_end = position + buf_size;
						// Record the amount of spare space available in this node.
						long space_available = LeafSpareSpace;
						// Is there a node immediately after we can shift the data into?
						bool successful = MoveToNextLeaf(key);
						if (successful) {
							// We were successful at moving to the next node, so determine if
							// there is space available here to make the shift
							if (LeafSpareSpace + space_available >= shift_offset) {
								// Yes there is, so lets make room,
								ShiftLeaf(shift_offset - space_available);
								// Move back
								SetupForPosition(key, position);
								// Expand this node to max size
								ExpandLeaf(space_available);
								// And copy,
								WriteFrom(key, position + shift_offset, buf, 0, buf_size);
								// Done,
								return;
							} else {
								// Not enough spare space available in the node with the
								// shift point and the next node, so we need to make new nodes,
								SetupForPosition(key, position);
								// Expand this node to max size
								ExpandLeaf(space_available);
								// Add nodes after it
								AddSpaceAfter(key, shift_offset - space_available);
								// And copy,
								WriteFrom(key, position + shift_offset, buf, 0, buf_size);
								// Done,
								return;
							}
						} else {
							// If we were unsuccessful at moving data to the next leaf, we must
							// be at the last node in the file.

							// Expand,
							long to_expand_by = shift_offset;
							to_expand_by -= ExpandLeaf(to_expand_by);
							// And add nodes for the remaining
							AddSpaceAfter(key, to_expand_by);
							// And copy,
							WriteFrom(key, position + shift_offset, buf, 0, buf_size);
							// Done,
							return;
						}
					}
						// shift_offset is < 0
					else {
						// We need to reduce,
						// The data to copy from the leaf
						int buf_size = LeafLength - LeafOffset;
						byte[] buf = new byte[buf_size];
						ReadInto(key, position, buf, 0, buf_size);
						// Set up to the point where we will be inserting the data into,
						SetupForPosition(key, position);

						// Delete all the nodes between the current node and the destination
						// node, but don't delete either the destination node or this node
						// in the process.
						long bytes_removed = DeleteAllNodesBackTo(key, position + shift_offset);

						// Position
						SetupForPosition(key, position + shift_offset);
						// Record the amount of spare space available in this node.
						long space_available = LeafSpareSpace;
						// Expand the leaf
						ExpandLeaf(space_available);
						// Will we be writing over two nodes?
						bool writing_over_two_nodes = buf_size > (LeafLength - LeafOffset);
						bool writing_complete_node = buf_size == (LeafLength - LeafOffset);
						// Write the data,
						WriteFrom(key, position + shift_offset, buf, 0, buf_size);
						// Move to the end of what we just inserted,
						SetupForPosition(key, position + shift_offset + buf_size);
						// Trim the node,
						if (!writing_complete_node) {
							TrimAtPosition();
						}
						if (!writing_over_two_nodes) {
							// Move to the end of what we just inserted,
							SetupForPosition(key, position + shift_offset + buf_size);
							// Delete this node
							DeleteLeaf(key);
						}
						// Finished
					}
				}
			}

			public void Reset() {
				Clear();
				current_leaf = null;
				current_leaf_key = null;
			}

		}

		#endregion

		#region TransactionDataFile

		private class TransactionDataFile : DataFile {
			private readonly TreeSystemTransaction tnx;
			private FileAccess access;
			private Key key;
			private long pos;

			private long version;
			private long start;
			private long end;

			private TreeStack stack;

			internal TransactionDataFile(TreeSystemTransaction tnx, Key key, FileAccess access) {
				this.tnx = tnx;
				stack = new TreeStack(tnx);
				this.key = key;
				pos = 0;

				version = -1;
				this.access = access;
				start = -1;
				end = -1;
			}

			private ITreeStorageSystem TreeSystem {
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
						end = tnx.KeyEndPosition(key);
						if (end < 0) {
							// When end is less than 0, the key was not found in the tree
							end = -(end + 1);
							start = end;
						} else {
							start = tnx.KeyStartPosition(key);
						}
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
				TreeStack target_stack;
				TreeStack source_stack;
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
					int to_copy = (int)Math.Min(size, source_stack.LeafLength - leaf_off);
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
						byte [] buf = new byte[to_copy];
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
						bool target_key_exists =
							target_stack.CurrentLeafKey.Equals(target_key);
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
					stack.ReadInto(key, start + pos, buffer, offset, count);
					pos += count;
					return count;
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

			#endregion
		}

		#endregion

		#region PlaceholderLeaf

		private class PlaceholderLeaf : TreeLeaf {
			private readonly TreeSystemTransaction tnx;
			private TreeLeaf leaf;
			private readonly long nodeId;
			private readonly int length;

			internal PlaceholderLeaf(TreeSystemTransaction tnx, long nodeId, int length) {
				this.tnx = tnx;
				this.nodeId = nodeId;
				this.length = length;
			}

			private TreeLeaf Leaf {
				get {
					if (leaf == null)
						leaf = (TreeLeaf) tnx.FetchNode(Id);
					return leaf;
				}
			}

			public override long Id {
				get { return nodeId; }
			}

			public override int Length {
				get { return length; }
			}

			public override void Read(int position, byte[] buf, int off, int len) {
				Leaf.Read(position, buf, off, len);
			}

			public override int Capacity {
				get { throw new NotSupportedException(); }
			}

			public override void Write(int position, byte[] buf, int off, int len) {
				Leaf.Write(position, buf, off, len);
			}

			public override void WriteTo(IAreaWriter writer) {
				Leaf.WriteTo(writer);
			}

			public override void SetLength(int size) {
				Leaf.SetLength(size);
			}

			public override void Shift(int position, int offset) {
				Leaf.Shift(position, offset);
			}

			public override long MemoryAmount {
				get { throw new NotSupportedException(); }
			}

		}

		#endregion
		#region KeyEnumerator

		private class KeyEnumerator : IEnumerator<Key> {
			private Key found_key;
			private Key last_key;
			private readonly TreeSystemTransaction tran;

			internal KeyEnumerator(TreeSystemTransaction tran) {
				this.tran = tran;
				// The last key is the head key, initially
				last_key = Key.Head;
				found_key = null;
			}

			private Key NextKey {
				get {
					if (found_key == null) {
						try {
							found_key = tran.NextKey(Key.Tail, last_key, tran.rootNodeId);
						} catch (IOException e) {
							// PENDING: It's bad to wrap this IO error - an IOException here
							// should be a critical stop condition.  Perhaps we need to
							// implement a key iterator that can throw an IOException.
							throw new Exception(e.Message, e);
						}
					}
					return found_key;
				}
			}

			#region IEnumerator<Key> Members

			public bool MoveNext() {
				return !NextKey.Equals(Key.Tail);
			}

			public void Reset() {
				last_key = Key.Head;
				found_key = null;
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public Key Current {
				get {
					Key next_key = NextKey;
					if (next_key.Equals(Key.Tail))
						throw new Exception("End of iterator");

					found_key = null;
					last_key = next_key;
					return next_key;
				}
			}

			#endregion

			#region Implementation of IDisposable

			void IDisposable.Dispose() {
			}

			#endregion
		}

		#endregion
	}
}