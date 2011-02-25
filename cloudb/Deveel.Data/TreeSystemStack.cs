using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	class TreeSystemStack {

		private readonly TreeSystemTransaction ts;

		private int stackSize;
		private long[] stack;

		private const int StackFrameSize = 4;

		private TreeLeaf currentLeaf;
		private Key currentLeafKey;
		private int leafOffset;

		internal TreeSystemStack(TreeSystemTransaction ts) {
			this.ts = ts;
			stackSize = 0;
			stack = new long[StackFrameSize * 13];
			currentLeaf = null;
			currentLeafKey = null;
			leafOffset = 0;
		}

		// Pass through methods to tree system transaction,

		private ITreeNode FetchNode(NodeId nodeId) {
			return ts.FetchNode(nodeId);
		}

		private ITreeNode UnfreezeNode(ITreeNode node) {
			return ts.UnfreezeNode(node);
		}

		private bool IsFrozen(NodeId nodeId) {
			return TreeSystemTransaction.IsFrozen(nodeId);
		}

		private bool IsHeapNode(NodeId nodeId) {
			return TreeSystemTransaction.IsHeapNode(nodeId);
		}

		private NodeId WriteNode(NodeId nodeId) {
			return ts.WriteNode(nodeId);
		}

		private void DeleteNode(NodeId nodeId) {
			ts.DeleteNode(nodeId);
		}

		private void RemoveAbsoluteBounds(long posStart, long posEnd) {
			ts.RemoveAbsoluteBounds(posStart, posEnd);
		}

		private TreeBranch CreateBranch() {
			return ts.CreateBranch();
		}

		private TreeLeaf CreateLeaf(Key key) {
			return ts.CreateLeaf(key);
		}

		private TreeLeaf CreateSparseLeaf(Key key, byte bt, long size) {
			return ts.CreateSparseLeaf(key, bt, size);
		}

		private ITreeSystem TreeSystem {
			get { return ts.TreeSystem; }
		}

		private int TreeHeight {
			set { ts.TreeHeight = value; }
			get { return ts.TreeHeight; }
		}

		private NodeId RootNodeId {
			set { ts.RootNodeId = value; }
			get { return ts.RootNodeId; }
		}

		private void StackPush(int child_i, long offset, NodeId nodeId) {
			if (stackSize + StackFrameSize >= stack.Length) {
				// Expand the size of the stack.
				// The default size should be plenty for most iterators unless we
				// happen to be iterating across a particularly deep B+Tree.
				long[] newStack = new long[stack.Length * 2];
				Array.Copy(stack, 0, newStack, 0, stack.Length);
				stack = newStack;
			}
			stack[stackSize] = child_i;
			stack[stackSize + 1] = offset;
			stack[stackSize + 2] = nodeId.High;
			stack[stackSize + 3] = nodeId.Low;
			stackSize += StackFrameSize;
		}

		private StackFrame StackEnd(int off) {
			return new StackFrame(stack, stackSize - ((off + 1) * StackFrameSize));
		}

		private StackFrame StackPop() {
			if (stackSize == 0) {
				throw new ApplicationException("Iterator stack underflow.");
			}
			stackSize -= StackFrameSize;
			return new StackFrame(stack, stackSize);
		}

		private int FrameCount {
			get { return stackSize/StackFrameSize; }
		}

		private bool StackEmpty {
			get { return (stackSize == 0); }
		}

		private void StackClear() {
			stackSize = 0;
		}

		private void UnfreezeStack() {
			StackFrame frame = StackEnd(0);

			NodeId oldChildNodeRef = frame.NodeId;
			// If the leaf ref isn't frozen then we exit early
			if (!IsFrozen(oldChildNodeRef)) {
				return;
			}
			TreeLeaf leaf = (TreeLeaf)UnfreezeNode(FetchNode(oldChildNodeRef));
			NodeId newChildNodeRef = leaf.Id;
			frame.NodeId = newChildNodeRef;
			currentLeaf = leaf;
			// NOTE: Setting currentLeaf here does not change the key of the node
			//   so we don't need to update currentLeafKey.

			// Walk the rest of the stack from the end
			int sz = FrameCount;
			for (int i = 1; i < sz; ++i) {
				int changed_child_i = frame.getChildI();
				frame = StackEnd(i);
				NodeId oldBranchRef = frame.NodeId;
				TreeBranch branch = (TreeBranch)UnfreezeNode(FetchNode(oldBranchRef));
				// Get the child_i from the stack,
				branch.SetChild(changed_child_i, newChildNodeRef);

				// Change the stack entry
				frame.NodeId = branch.Id;

				newChildNodeRef = branch.Id;
			}

			// Set the new root node ref
			RootNodeId = newChildNodeRef;
		}

		public void WriteLeafOnly(Key key) {
			// Get the stack frame for the last entry.
			StackFrame frame = StackEnd(0);
			// The leaf
			NodeId leafRef = frame.NodeId;
			// Write it out
			NodeId newRef = WriteNode(leafRef);
			// If new_ref = leaf_ref, then we didn't write a new node
			if (newRef.Equals(leafRef)) {
				return;
			} else {
				// Otherwise, update the references,
				frame.NodeId = newRef;
				currentLeaf = (TreeLeaf)FetchNode(newRef);
				// Walk back up the stack and update the ref as necessary
				int sz = FrameCount;
				for (int i = 1; i < sz; ++i) {
					// Get the child_i from the stack,
					int changed_child_i = frame.getChildI();

					frame = StackEnd(i);

					NodeId old_branch_ref = frame.NodeId;
					TreeBranch branch =
								 (TreeBranch)UnfreezeNode(FetchNode(old_branch_ref));
					branch.SetChild(changed_child_i, newRef);

					// Change the stack entry
					newRef = branch.Id;
					frame.NodeId = newRef;
				}

				// Set the new root node ref
				RootNodeId = newRef;
			}
		}

		private void UpdateStackProperties(int sizeDiff) {
			StackFrame frame = StackEnd(0);
			int sz = FrameCount;
			// Walk the stack from the end
			for (int i = 1; i < sz; ++i) {
				int child_i = frame.getChildI();
				frame = StackEnd(i);

				NodeId node_ref = frame.NodeId;
				TreeBranch branch = (TreeBranch)FetchNode(node_ref);
				branch.SetChildLeafElementCount(child_i, branch.GetChildLeafElementCount(child_i) + sizeDiff);
			}
		}

		public void InsertLeaf(Key newLeafKey, TreeLeaf newLeaf, bool before) {
			int leaf_size = newLeaf.Length;
			if (leaf_size <= 0) {
				throw new ApplicationException("size <= 0");
			}

			// The current absolute position and key
			Key newKey = newLeafKey;

			// The frame at the end of the stack,
			StackFrame frame = StackEnd(0);


			Object[] nfo;
			Object[] r_nfo = new Object[5];
			Key leftKey;
			long cur_absolute_pos;
			// If we are inserting the new leaf after,
			if (!before) {
				nfo = new Object[] {
                currentLeaf.Id,
                (long) currentLeaf.Length,
                newLeafKey,
                newLeaf.Id,
                (long) newLeaf.Length };
				leftKey = null;
				cur_absolute_pos = frame.Offset + currentLeaf.Length;
			}
				// Otherwise we are inserting the new leaf before,
			else {
				// If before and currentLeaf key is different than new_leaf key, we
				// generate an error
				if (!currentLeafKey.Equals(newLeafKey)) {
					throw new ApplicationException("Can't insert different new key before.");
				}
				nfo = new Object[] {
                newLeaf.Id,
                (long) newLeaf.Length,
                currentLeafKey,
                currentLeaf.Id,
                (long) currentLeaf.Length };
				leftKey = newLeafKey;
				cur_absolute_pos = frame.Offset - 1;
			}

			bool insert_two_nodes = true;

			int sz = FrameCount;
			// Walk the stack from the end
			for (int i = 1; i < sz; ++i) {
				// child_i is the previous frame's child_i
				int child_i = frame.getChildI();
				frame = StackEnd(i);
				// The child ref of this stack element
				NodeId child_ref = frame.NodeId;
				// Fetch it
				TreeBranch branch = (TreeBranch)UnfreezeNode(FetchNode(child_ref));

				// Do we have two nodes to insert into the branch?
				if (insert_two_nodes) {
					TreeBranch insert_branch;
					int insert_n = child_i;
					// If the branch is full,
					if (branch.IsFull) {
						// Create a new node,
						TreeBranch leftBranch = branch;
						TreeBranch rightBranch = CreateBranch();
						// Split the branch,
						Key midpointKey = leftBranch.MidPointKey;
						// And move half of this branch into the new branch
						leftBranch.MoveLastHalfInto(rightBranch);
						// We split so we need to return a split flag,
						r_nfo[0] = leftBranch.Id;
						r_nfo[1] = leftBranch.LeafElementCount;
						r_nfo[2] = midpointKey;
						r_nfo[3] = rightBranch.Id;
						r_nfo[4] = rightBranch.LeafElementCount;
						// Adjust insert_n and insert_branch
						if (insert_n >= leftBranch.ChildCount) {
							insert_n -= leftBranch.ChildCount;
							insert_branch = rightBranch;
							r_nfo[4] = (long)r_nfo[4] + newLeaf.Length;
							// If insert_n == 0, we change the midpoint value to the left
							// key value,
							if (insert_n == 0 && leftKey != null) {
								r_nfo[2] = leftKey;
								leftKey = null;
							}
						} else {
							insert_branch = leftBranch;
							r_nfo[1] = (long)r_nfo[1] + newLeaf.Length;
						}
					}
						// If it's not full,
					else {
						insert_branch = branch;
						r_nfo[0] = insert_branch.Id;
						insert_two_nodes = false;
					}
					// Insert the two children nodes
					insert_branch.Insert((NodeId)nfo[0], (long)nfo[1],
										  (Key)nfo[2],
										  (NodeId)nfo[3], (long)nfo[4],
										  insert_n);
					// Copy r_nfo to nfo
					for (int p = 0; p < r_nfo.Length; ++p) {
						nfo[p] = r_nfo[p];
					}

					// Adjust the left key reference if necessary
					if (leftKey != null && insert_n > 0) {
						insert_branch.SetKeyValueToLeft(leftKey, insert_n);
						leftKey = null;
					}
				} else {
					branch.SetChild(child_i, (NodeId)nfo[0]);
					nfo[0] = branch.Id;
					branch.SetChildLeafElementCount(child_i, branch.GetChildLeafElementCount(child_i) + leaf_size);

					// Adjust the left key reference if necessary
					if (leftKey != null && child_i > 0) {
						branch.SetKeyValueToLeft(leftKey, child_i);
						leftKey = null;
					}
				}

			} // For all elements in the stack,

			// At the end, if we still have a split then we make a new root and
			// adjust the stack accordingly
			if (insert_two_nodes) {
				TreeBranch new_root = CreateBranch();
				new_root.Set((NodeId)nfo[0], (long)nfo[1],
							 (Key)nfo[2],
							 (NodeId)nfo[3], (long)nfo[4]);
				RootNodeId = new_root.Id;
				if (TreeHeight != -1) {
					TreeHeight = TreeHeight + 1;
				}
			} else {
				RootNodeId = (NodeId)nfo[0];
			}

			// Now reset the position,
			Reset();
			SetupForPosition(newKey, cur_absolute_pos);
		}

		private bool RedistributeBranchElements(TreeBranch branch, int child_i, TreeBranch child) {
			// We distribute the nodes in the child branch with the branch
			// immediately to the right.  If that's not possible, then we distribute
			// with the left.

			// If branch has only a single value, return immediately
			int branch_size = branch.ChildCount;
			if (branch_size == 1) {
				return false;
			}

			int left_i, right_i;
			TreeBranch left, right;
			if (child_i < branch_size - 1) {
				// Distribute with the right
				left_i = child_i;
				right_i = child_i + 1;
				left = child;
				right = (TreeBranch)UnfreezeNode(FetchNode(branch.GetChild(child_i + 1)));
				branch.SetChild(child_i + 1, right.Id);
			} else {
				// Distribute with the left
				left_i = child_i - 1;
				right_i = child_i;
				left = (TreeBranch)UnfreezeNode(FetchNode(branch.GetChild(child_i - 1)));
				right = child;
				branch.SetChild(child_i - 1, left.Id);
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
				DeleteNode(right.Id);
				// And remove it from the branch,
				branch.RemoveChild(right_i);
				return true;
			} else {
				// Otherwise set the key reference
				branch.SetKeyValueToLeft(new_mid_key, right_i);
				return false;
			}

		}

		public TreeLeaf CurrentLeaf {
			get { return currentLeaf; }
		}

		public Key CurrentLeafKey {
			get { return currentLeafKey; }
		}

		public int LeafOffset {
			get { return leafOffset; }
		}

		public bool IsAtEndOfKeyData {
			get { return LeafOffset >= LeafSize; }
		}

		public void SetupForPosition(Key key, long posit) {

			// If the current leaf is set
			if (currentLeaf != null) {
				StackFrame frame = StackEnd(0);
				long leaf_start = frame.Offset;
				long leaf_end = leaf_start + currentLeaf.Length;
				// If the position is at the leaf end, or if the keys aren't equal, we
				// need to reset the stack.  This ensures that we correctly place the
				// pointer.
				if (!key.Equals(Key.Tail) &&
						(posit == leaf_end || !key.Equals(currentLeafKey))) {
					StackClear();
					currentLeaf = null;
					currentLeafKey = null;
				} else {
					// Check whether the position is within the bounds of the current leaf
					// If 'posit' is within this leaf
					if (posit >= leaf_start && posit < leaf_end) {
						// If the position is within the current leaf, set up the internal
						// vars as necessary.
						leafOffset = (int)(posit - leaf_start);
						return;
					} else {
						// If it's not, we reset the stack and start fresh,
						StackClear();
						currentLeaf = null;
						currentLeafKey = null;
					}
				}
			}

			// ISSUE: It appears looking at the code above, the stack will always be
			//   empty and currentLeaf will always be null if we get here.

			// If the stack is empty, push the root node,
			if (StackEmpty) {
				// Push the root node onto the top of the stack.
				StackPush(-1, 0, RootNodeId);
				// Set up the currentLeafKey to the default value
				currentLeafKey = Key.Head;
			}
			// Otherwise, we need to setup by querying the BTree.
			while (true) {
				if (StackEmpty) {
					throw new ApplicationException("Position out of bounds.  p = " + posit);
				}

				// Pop the last stack frame,
				StackFrame frame = StackPop();
				NodeId node_pointer = frame.NodeId;
				long left_side_offset = frame.Offset;
				int node_child_i = frame.getChildI();
				// Relative offset within this node
				long relative_offset = posit - left_side_offset;

				// If the node is not on the heap,
				if (!IsHeapNode(node_pointer)) {
					// The node is not on the heap. We optimize here.
					// If we know the node is going to be a leaf node, we set up a
					// temporary leaf node object with as much information as we know.

					// Check if we know this is a leaf
					int tree_height = TreeHeight;
					if (tree_height != -1 &&
						(stackSize / StackFrameSize) + 1 == tree_height) {

						// Fetch the parent node,
						frame = StackEnd(0);
						NodeId twig_node_pointer = frame.NodeId;
						TreeBranch twig = (TreeBranch)FetchNode(twig_node_pointer);
						long leaf_size = twig.GetChildLeafElementCount((int)node_child_i);


						// This object holds off fetching the contents of the leaf node
						// unless it's absolutely required.
						TreeLeaf leaf = new PlaceholderLeaf(ts, node_pointer, (int)leaf_size);

						currentLeaf = leaf;
						StackPush(node_child_i, left_side_offset, node_pointer);
						// Set up the leaf offset and return
						leafOffset = (int)relative_offset;

						return;
					}
				}

				// Fetch the node
				ITreeNode node = FetchNode(node_pointer);
				if (node is TreeLeaf) {
					// Node is a leaf node
					TreeLeaf leaf = (TreeLeaf)node;

					currentLeaf = leaf;
					StackPush(node_child_i, left_side_offset, node_pointer);
					// Set up the leaf offset and return
					leafOffset = (int)relative_offset;

					// Update the tree_height value,
					TreeHeight = stackSize / StackFrameSize;
					//          tree_height = (stack_size / STACK_FRAME_SIZE);
					return;
				} else {
					// Node is a branch node
					TreeBranch branch = (TreeBranch)node;
					int child_i = branch.IndexOfChild(key, relative_offset);
					if (child_i != -1) {
						// Push the current details,
						StackPush(node_child_i, left_side_offset, node_pointer);
						// Found child so push the details
						StackPush(child_i,
								  branch.IndexOfChild(child_i) + left_side_offset,
								  branch.GetChild(child_i));
						// Set up the left key
						if (child_i > 0) {
							currentLeafKey = branch.GetKey(child_i);
						}
					}
				}
			} // while (true)
		}

		public void DeleteLeaf(Key key) {
			// Set up the state
			StackFrame frame = StackEnd(0);
			NodeId this_ref = frame.NodeId;
			TreeBranch branch_node = null;
			int delete_node_size = -1;
			Key left_key = null;

			// Walk back through the rest of the stack
			int sz = FrameCount;
			for (int i = 1; i < sz; ++i) {

				// Find the child_i for the child
				// This is the child_i of the child in the current branch
				int child_i = frame.getChildI();

				// Move the stack frame,
				frame = StackEnd(i);

				NodeId child_ref = this_ref;
				this_ref = frame.NodeId;
				TreeBranch child_branch = branch_node;
				branch_node = (TreeBranch)UnfreezeNode(FetchNode(this_ref));

				if (delete_node_size == -1) {
					delete_node_size = (int)branch_node.GetChildLeafElementCount(child_i);
				}

				// If the child branch is empty,
				if (child_branch == null || child_branch.IsEmpty) {
					// Delete the reference to it,
					if (child_i == 0 && branch_node.ChildCount > 1) {
						left_key = branch_node.GetKey(1);
					}
					branch_node.RemoveChild(child_i);
					// Delete the child branch,
					DeleteNode(child_ref);
				}
					// Not empty,
				else {
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

			}

			// Finally, set the root node
			// If the branch node is a single element, we set the root as the child,
			if (branch_node.ChildCount == 1) {
				// This shrinks the height of the tree,
				RootNodeId = branch_node.GetChild(0);
				DeleteNode(branch_node.Id);
				if (TreeHeight != -1) {
					TreeHeight = TreeHeight - 1;
				}
			} else {
				// Otherwise, we set the branch node.
				RootNodeId = branch_node.Id;
			}

			// Reset the object
			Reset();

		}

		public void RemoveSpaceBefore(Key key, long amountToRemove) {
			StackFrame frame = StackEnd(0);
			long p = (frame.Offset + leafOffset) - 1;
			while (true) {
				SetupForPosition(key, p);
				int leaf_size = LeafSize;
				if (amountToRemove >= leaf_size) {
					DeleteLeaf(key);
					amountToRemove -= leaf_size;
				} else {
					if (amountToRemove > 0) {
						SetupForPosition(key, (p - amountToRemove) + 1);
						TrimAtPosition();
					}
					return;
				}
				p -= leaf_size;
			}
		}

		public long DeleteAllNodesBackTo(Key key, long backPosition) {
			StackFrame frame = StackEnd(0);

			long p = frame.Offset - 1;
			long bytes_removed = 0;

			while (true) {
				// Set up for the node,
				SetupForPosition(key, p);
				// This is the stopping condition, when the start of the node is
				// before the back_position,
				if (StackEnd(0).Offset <= backPosition) {
					return bytes_removed;
				}
				// The current leaf size
				int leaf_size = LeafSize;
				// The bytes removed is the size of the leaf,
				bytes_removed += LeafSize;
				// Otherwise, delete the leaf
				DeleteLeaf(key);

				p -= leaf_size;
			}
		}

		public void SplitLeaf(Key key, long position) {
			UnfreezeStack();
			TreeLeaf source_leaf = CurrentLeaf;
			int split_point = LeafOffset;
			// The amount of data we are copying from the current key.
			int amount = source_leaf.Length - split_point;
			// Create a new empty node
			TreeLeaf empty_leaf = CreateLeaf(key);
			empty_leaf.SetLength(amount);
			// Copy the data at the end of the leaf into a buffer
			byte[] buf = new byte[amount];
			source_leaf.Read(split_point, buf, 0, amount);
			// And write it out to the new leaf
			empty_leaf.Write(0, buf, 0, amount);
			// Set the new size of the node
			source_leaf.SetLength(split_point);
			// Update the stack properties
			UpdateStackProperties(-amount);
			// And insert the new leaf after
			InsertLeaf(key, empty_leaf, false);
		}

		public void AddSpaceAfter(Key key, long space_to_add) {
			while (space_to_add > 0) {
				// Create an empty sparse node
				TreeLeaf empty_leaf = CreateSparseLeaf(key, (byte)0, space_to_add);
				InsertLeaf(key, empty_leaf, false);
				space_to_add -= empty_leaf.Length;
			}
		}

		public int ExpandLeaf(long amount) {
			if (amount > 0) {
				UnfreezeStack();
				int actual_expand_by = (int)Math.Min(
						  (long)currentLeaf.Capacity - currentLeaf.Length,
						  amount);
				if (actual_expand_by > 0) {
					currentLeaf.SetLength(currentLeaf.Length + actual_expand_by);
					UpdateStackProperties(actual_expand_by);
				}
				return actual_expand_by;
			}
			return 0;
		}

		public void TrimAtPosition() {
			UnfreezeStack();
			int size_before = currentLeaf.Length;
			currentLeaf.SetLength(leafOffset);
			UpdateStackProperties(leafOffset - size_before);
		}

		public void ShiftLeaf(long amount) {
			if (amount != 0) {
				UnfreezeStack();
				int size_before = currentLeaf.Length;
				currentLeaf.Shift(leafOffset, (int)amount);
				UpdateStackProperties(currentLeaf.Length - size_before);
			}
		}

		public bool MoveToNextLeaf(Key key) {
			long next_pos = StackEnd(0).Offset + currentLeaf.Length;
			SetupForPosition(key, next_pos);
			return leafOffset != currentLeaf.Length &&
				   currentLeafKey.Equals(key);
		}

		public bool MoveToPreviousLeaf(Key key) {
			long previous_pos = StackEnd(0).Offset - 1;
			SetupForPosition(key, previous_pos);
			return currentLeafKey.Equals(key);
		}

		public int LeafSize {
			get { return currentLeaf.Length; }
		}

		public int LeafSpareSpace {
			get { return TreeSystem.MaxLeafByteSize - currentLeaf.Length; }
		}

		public int ReadInto(Key key, long current_p, byte[] buf, int off, int len) {
			int read = 0;
			// While there is information to read into the array,
			while (len > 0) {
				// Set up the stack and internal variables for the given position,
				SetupForPosition(key, current_p);
				// Read as much as we can from the current leaf capped at the leaf size
				// if necessary,
				int toRead = Math.Min(len, currentLeaf.Length - leafOffset);
				if (toRead == 0) {
					throw new ApplicationException("Read out of bounds.");
				}
				// Copy the leaf into the array,
				currentLeaf.Read(leafOffset, buf, off, toRead);
				// Modify the pointers
				current_p += toRead;
				off += toRead;
				len -= toRead;
				read += toRead;
			}

			return read;
		}

		public void WriteFrom(Key key, long current_p, byte[] buf, int off, int len) {
			// While there is information to read into the array,
			while (len > 0) {
				// Set up the stack and internal variables for the given position,
				SetupForPosition(key, current_p);
				// Unfreeze all the nodes currently on the stack,
				UnfreezeStack();
				// Read as much as we can from the current leaf capped at the leaf size
				// if necessary,
				int to_write = Math.Min(len, currentLeaf.Length - leafOffset);
				if (to_write == 0) {
					throw new ApplicationException("Write out of bounds.");
				}
				// Copy the leaf into the array,
				currentLeaf.Write(leafOffset, buf, off, to_write);
				// Modify the pointers
				current_p += to_write;
				off += to_write;
				len -= to_write;
			}
		}

		public void ShiftData(Key key, long position, long shift_offset) {
			// Return if nothing being shifted
			if (shift_offset == 0) {
				return;
			}

			// If we are removing a large amount of data,
			// TODO: Rather arbitrary value here...
			if (shift_offset < -(32 * 1024)) {
				// If removing more than 32k of data use the generalized tree pruning
				// algorithm which works well on large data.
				Reset();
				RemoveAbsoluteBounds(position + shift_offset, position);
			}
				// shift_offset > 0
			else {
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
						int buf_size = LeafSize - LeafOffset;
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
						int buf_size = LeafSize - LeafOffset;
						byte[] buf = new byte[buf_size];
						ReadInto(key, position, buf, 0, buf_size);
						// Set up to the point where we will be inserting the data into,
						SetupForPosition(key, position);

						// Delete all the nodes between the current node and the destination
						// node, but don't delete either the destination node or this node
						// in the process.
						long bytes_removed = DeleteAllNodesBackTo(key,
																   position + shift_offset);

						// Position
						SetupForPosition(key, position + shift_offset);
						// Record the amount of spare space available in this node.
						long space_available = LeafSpareSpace;
						// Expand the leaf
						ExpandLeaf(space_available);
						// Will we be writing over two nodes?
						bool writing_over_two_nodes =
									 buf_size > (LeafSize - LeafOffset);
						bool writing_complete_node =
									buf_size == (LeafSize - LeafOffset);
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
		}




		public void Reset() {
			StackClear();
			currentLeaf = null;
			currentLeafKey = null;
		}


		private class PlaceholderLeaf : TreeLeaf {

			private readonly TreeSystemTransaction ts;

			private TreeLeaf actual_leaf;
			private NodeId node_ref;
			private int size;

			public PlaceholderLeaf(TreeSystemTransaction ts,
							NodeId node_ref, int size)
				: base() {
				this.ts = ts;
				this.node_ref = node_ref;
				this.size = size;
			}

			private TreeLeaf Leaf {
				get {
					if (actual_leaf == null) {
						actual_leaf = (TreeLeaf) ts.FetchNode(Id);
					}
					return actual_leaf;
				}
			}

			public override NodeId Id {
				get { return node_ref; }
			}

			public override int Length {
				get { return size; }
			}

			public override void Read(int position, byte[] buf, int off, int len) {
				Leaf.Read(position, buf, off, len);
			}

			public override int Capacity {
				get {
					// Not supported, this object will never be a leaf node.
					throw new NotSupportedException();
				}
			}

			public override void Write(int position, byte[] buf, int off, int len) {
				Leaf.Write(position, buf, off, len);
			}

			public override void SetLength(int size) {
				Leaf.SetLength(size);
			}

			public override void Shift(int position, int offset) {
				Leaf.Shift(position, offset);
			}

			public override void WriteTo(IAreaWriter area) {
				Leaf.WriteTo(area);
			}

			public override long MemoryAmount {
				get { throw new NotSupportedException(); }
			}
		}

		private class StackFrame {

			private readonly long[] stack;
			private readonly int off;

			public StackFrame(long[] stack, int off) {
				this.stack = stack;
				this.off = off;
			}

			public int getChildI() {
				return (int)stack[off];
			}

			public long Offset {
				get { return stack[off + 1]; }
			}

			public NodeId NodeId {
				get { return new NodeId(stack[off + 2], stack[off + 3]); }
				set {
					stack[off + 2] = value.High;
					stack[off + 3] = value.Low;
				}
			}
		}
	}
}