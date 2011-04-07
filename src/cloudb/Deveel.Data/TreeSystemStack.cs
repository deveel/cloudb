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

		private int FrameCount {
			get { return stackSize/StackFrameSize; }
		}

		private bool StackEmpty {
			get { return (stackSize == 0); }
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

		public int LeafSize {
			get { return currentLeaf.Length; }
		}

		public int LeafSpareSpace {
			get { return TreeSystem.MaxLeafByteSize - currentLeaf.Length; }
		}

		private ITreeNode FetchNode(NodeId nodeId) {
			return ts.FetchNode(nodeId);
		}

		private ITreeNode UnfreezeNode(ITreeNode node) {
			return ts.UnfreezeNode(node);
		}

		private static bool IsFrozen(NodeId nodeId) {
			return TreeSystemTransaction.IsFrozen(nodeId);
		}

		private static bool IsHeapNode(NodeId nodeId) {
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

		private void StackPush(int childIndex, long offset, NodeId nodeId) {
			if (stackSize + StackFrameSize >= stack.Length) {
				// Expand the size of the stack.
				// The default size should be plenty for most iterators unless we
				// happen to be iterating across a particularly deep B+Tree.
				long[] newStack = new long[stack.Length * 2];
				Array.Copy(stack, 0, newStack, 0, stack.Length);
				stack = newStack;
			}
			stack[stackSize + 0] = childIndex;
			stack[stackSize + 1] = offset;
			stack[stackSize + 2] = nodeId.High;
			stack[stackSize + 3] = nodeId.Low;
			stackSize += StackFrameSize;
		}

		private StackFrame StackEnd(int off) {
			return new StackFrame(stack, stackSize - ((off + 1) * StackFrameSize));
		}

		private StackFrame StackPop() {
			if (stackSize == 0)
				throw new ApplicationException("Iterator stack underflow.");

			stackSize -= StackFrameSize;
			return new StackFrame(stack, stackSize);
		}

		private void StackClear() {
			stackSize = 0;
		}

		private void UnfreezeStack() {
			StackFrame frame = StackEnd(0);

			NodeId oldChildNodeRef = frame.NodeId;
			// If the leaf ref isn't frozen then we exit early
			if (!IsFrozen(oldChildNodeRef))
				return;

			TreeLeaf leaf = (TreeLeaf)UnfreezeNode(FetchNode(oldChildNodeRef));
			NodeId newChildNodeRef = leaf.Id;
			frame.NodeId = newChildNodeRef;
			currentLeaf = leaf;
			// NOTE: Setting currentLeaf here does not change the key of the node
			//   so we don't need to update currentLeafKey.

			// Walk the rest of the stack from the end
			int sz = FrameCount;
			for (int i = 1; i < sz; ++i) {
				int changedChildIndex = frame.ChildIndex;
				frame = StackEnd(i);
				NodeId oldBranchRef = frame.NodeId;
				TreeBranch branch = (TreeBranch)UnfreezeNode(FetchNode(oldBranchRef));
				// Get the child_i from the stack,
				branch.SetChild(changedChildIndex, newChildNodeRef);

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
			// If newRef = leafRef, then we didn't write a new node
			if (newRef.Equals(leafRef))
				return;

			// Otherwise, update the references,
			frame.NodeId = newRef;
			currentLeaf = (TreeLeaf) FetchNode(newRef);
			// Walk back up the stack and update the ref as necessary
			int sz = FrameCount;
			for (int i = 1; i < sz; ++i) {
				// Get the child index from the stack,
				int changedChildIndex = frame.ChildIndex;

				frame = StackEnd(i);

				NodeId oldBranchRef = frame.NodeId;
				TreeBranch branch =(TreeBranch) UnfreezeNode(FetchNode(oldBranchRef));
				branch.SetChild(changedChildIndex, newRef);

				// Change the stack entry
				newRef = branch.Id;
				frame.NodeId = newRef;
			}

			// Set the new root node ref
			RootNodeId = newRef;
		}

		private void UpdateStackProperties(int sizeDiff) {
			StackFrame frame = StackEnd(0);
			int sz = FrameCount;
			// Walk the stack from the end
			for (int i = 1; i < sz; ++i) {
				int childIndex = frame.ChildIndex;
				frame = StackEnd(i);

				NodeId nodeRef = frame.NodeId;
				TreeBranch branch = (TreeBranch)FetchNode(nodeRef);
				branch.SetChildLeafElementCount(childIndex, branch.GetChildLeafElementCount(childIndex) + sizeDiff);
			}
		}

		public void InsertLeaf(Key newLeafKey, TreeLeaf newLeaf, bool before) {
			int leafSize = newLeaf.Length;
			if (leafSize <= 0)
				throw new ApplicationException("size <= 0");

			// The current absolute position and key
			Key newKey = newLeafKey;

			// The frame at the end of the stack,
			StackFrame frame = StackEnd(0);


			object[] nfo;
			object[] rNfo = new Object[5];
			Key leftKey;
			long curAbsolutePos;
			// If we are inserting the new leaf after,
			if (!before) {
				nfo = new object[] {
				                   	currentLeaf.Id,
				                   	(long) currentLeaf.Length,
				                   	newLeafKey,
				                   	newLeaf.Id,
				                   	(long) newLeaf.Length
				                   };
				leftKey = null;
				curAbsolutePos = frame.Offset + currentLeaf.Length;
			}
				// Otherwise we are inserting the new leaf before,
			else {
				// If before and currentLeaf key is different than new_leaf key, we
				// generate an error
				if (!currentLeafKey.Equals(newLeafKey))
					throw new ApplicationException("Can't insert different new key before.");

				nfo = new object[] {
				                   	newLeaf.Id,
				                   	(long) newLeaf.Length,
				                   	currentLeafKey,
				                   	currentLeaf.Id,
				                   	(long) currentLeaf.Length
				                   };
				leftKey = newLeafKey;
				curAbsolutePos = frame.Offset - 1;
			}

			bool insertTwoNodes = true;

			int sz = FrameCount;
			// Walk the stack from the end
			for (int i = 1; i < sz; ++i) {
				// child_i is the previous frame's child_i
				int childIndex = frame.ChildIndex;
				frame = StackEnd(i);
				// The child ref of this stack element
				NodeId childRef = frame.NodeId;
				// Fetch it
				TreeBranch branch = (TreeBranch)UnfreezeNode(FetchNode(childRef));

				// Do we have two nodes to insert into the branch?
				if (insertTwoNodes) {
					TreeBranch insertBranch;
					int insertN = childIndex;
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
						rNfo[0] = leftBranch.Id;
						rNfo[1] = leftBranch.LeafElementCount;
						rNfo[2] = midpointKey;
						rNfo[3] = rightBranch.Id;
						rNfo[4] = rightBranch.LeafElementCount;
						// Adjust insert_n and insert_branch
						if (insertN >= leftBranch.ChildCount) {
							insertN -= leftBranch.ChildCount;
							insertBranch = rightBranch;
							rNfo[4] = (long)rNfo[4] + newLeaf.Length;
							// If insert_n == 0, we change the midpoint value to the left
							// key value,
							if (insertN == 0 && leftKey != null) {
								rNfo[2] = leftKey;
								leftKey = null;
							}
						} else {
							insertBranch = leftBranch;
							rNfo[1] = (long)rNfo[1] + newLeaf.Length;
						}
					}
						// If it's not full,
					else {
						insertBranch = branch;
						rNfo[0] = insertBranch.Id;
						insertTwoNodes = false;
					}

					// Insert the two children nodes
					insertBranch.Insert((NodeId) nfo[0], (long) nfo[1], (Key) nfo[2], (NodeId) nfo[3], (long) nfo[4], insertN);

					// Copy r_nfo to nfo
					for (int p = 0; p < rNfo.Length; ++p) {
						nfo[p] = rNfo[p];
					}

					// Adjust the left key reference if necessary
					if (leftKey != null && insertN > 0) {
						insertBranch.SetKeyValueToLeft(leftKey, insertN);
						leftKey = null;
					}
				} else {
					branch.SetChild(childIndex, (NodeId)nfo[0]);
					nfo[0] = branch.Id;
					branch.SetChildLeafElementCount(childIndex, branch.GetChildLeafElementCount(childIndex) + leafSize);

					// Adjust the left key reference if necessary
					if (leftKey != null && childIndex > 0) {
						branch.SetKeyValueToLeft(leftKey, childIndex);
						leftKey = null;
					}
				}

			} // For all elements in the stack,

			// At the end, if we still have a split then we make a new root and
			// adjust the stack accordingly
			if (insertTwoNodes) {
				TreeBranch newRoot = CreateBranch();
				newRoot.Set((NodeId)nfo[0], (long)nfo[1], (Key)nfo[2], (NodeId)nfo[3], (long)nfo[4]); 
				RootNodeId = newRoot.Id;
				if (TreeHeight != -1) {
					TreeHeight = TreeHeight + 1;
				}
			} else {
				RootNodeId = (NodeId)nfo[0];
			}

			// Now reset the position,
			Reset();
			SetupForPosition(newKey, curAbsolutePos);
		}

		private bool RedistributeBranchElements(TreeBranch branch, int childIndex, TreeBranch child) {
			// We distribute the nodes in the child branch with the branch
			// immediately to the right.  If that's not possible, then we distribute
			// with the left.

			// If branch has only a single value, return immediately
			int branch_size = branch.ChildCount;
			if (branch_size == 1)
				return false;

			int leftIndex, rightIndex;
			TreeBranch left, right;
			if (childIndex < branch_size - 1) {
				// Distribute with the right
				leftIndex = childIndex;
				rightIndex = childIndex + 1;
				left = child;
				right = (TreeBranch) UnfreezeNode(FetchNode(branch.GetChild(childIndex + 1)));
				branch.SetChild(childIndex + 1, right.Id);
			} else {
				// Distribute with the left
				leftIndex = childIndex - 1;
				rightIndex = childIndex;
				left = (TreeBranch) UnfreezeNode(FetchNode(branch.GetChild(childIndex - 1)));
				right = child;
				branch.SetChild(childIndex - 1, left.Id);
			}

			// Get the mid value key reference
			Key midKey = branch.GetKey(rightIndex);

			// Perform the merge,
			Key newMidKey = left.Merge(right, midKey);
			// Reset the leaf element count
			branch.SetChildLeafElementCount(leftIndex, left.LeafElementCount);
			branch.SetChildLeafElementCount(rightIndex, right.LeafElementCount);

			// If after the merge the right branch is empty, we need to remove it
			if (right.IsEmpty) {
				// Delete the node
				DeleteNode(right.Id);
				// And remove it from the branch,
				branch.RemoveChild(rightIndex);
				return true;
			}

			// Otherwise set the key reference
			branch.SetKeyValueToLeft(newMidKey, rightIndex);
			return false;
		}

		public void SetupForPosition(Key key, long posit) {
			// If the current leaf is set
			if (currentLeaf != null) {
				StackFrame frame = StackEnd(0);
				long leafStart = frame.Offset;
				long leafEnd = leafStart + currentLeaf.Length;
				// If the position is at the leaf end, or if the keys aren't equal, we
				// need to reset the stack.  This ensures that we correctly place the
				// pointer.
				if (!key.Equals(Key.Tail) &&
				    (posit == leafEnd || !key.Equals(currentLeafKey))) {
					StackClear();
					currentLeaf = null;
					currentLeafKey = null;
				} else {
					// Check whether the position is within the bounds of the current leaf
					// If 'posit' is within this leaf
					if (posit >= leafStart && posit < leafEnd) {
						// If the position is within the current leaf, set up the internal
						// vars as necessary.
						leafOffset = (int) (posit - leafStart);
						return;
					}

					// If it's not, we reset the stack and start fresh,
					StackClear();
					currentLeaf = null;
					currentLeafKey = null;
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
				if (StackEmpty)
					throw new ApplicationException("Position out of bounds.  p = " + posit);

				// Pop the last stack frame,
				StackFrame frame = StackPop();
				NodeId nodePointer = frame.NodeId;
				long leftSideOffset = frame.Offset;
				int nodeChildIndex = frame.ChildIndex;
				// Relative offset within this node
				long relativeOffset = posit - leftSideOffset;

				// If the node is not on the heap,
				if (!IsHeapNode(nodePointer)) {
					// The node is not on the heap. We optimize here.
					// If we know the node is going to be a leaf node, we set up a
					// temporary leaf node object with as much information as we know.

					// Check if we know this is a leaf
					int treeHeight = TreeHeight;
					if (treeHeight != -1 &&
					    (stackSize/StackFrameSize) + 1 == treeHeight) {

						// Fetch the parent node,
						frame = StackEnd(0);
						NodeId twigNodePointer = frame.NodeId;
						TreeBranch twig = (TreeBranch) FetchNode(twigNodePointer);
						long leafSize = twig.GetChildLeafElementCount(nodeChildIndex);


						// This object holds off fetching the contents of the leaf node
						// unless it's absolutely required.
						TreeLeaf leaf = new PlaceholderLeaf(ts, nodePointer, (int) leafSize);

						currentLeaf = leaf;
						StackPush(nodeChildIndex, leftSideOffset, nodePointer);
						// Set up the leaf offset and return
						leafOffset = (int) relativeOffset;
						return;
					}
				}

				// Fetch the node
				ITreeNode node = FetchNode(nodePointer);
				if (node is TreeLeaf) {
					// Node is a leaf node
					TreeLeaf leaf = (TreeLeaf) node;

					currentLeaf = leaf;
					StackPush(nodeChildIndex, leftSideOffset, nodePointer);
					// Set up the leaf offset and return
					leafOffset = (int) relativeOffset;

					// Update the tree_height value,
					TreeHeight = stackSize/StackFrameSize;
					return;
				}

				// Node is a branch node
				TreeBranch branch = (TreeBranch) node;
				int childIndex = branch.IndexOfChild(key, relativeOffset);
				if (childIndex != -1) {
					// Push the current details,
					StackPush(nodeChildIndex, leftSideOffset, nodePointer);
					// Found child so push the details
					StackPush(childIndex, branch.IndexOfChild(childIndex) + leftSideOffset, branch.GetChild(childIndex));
					// Set up the left key
					if (childIndex > 0)
						currentLeafKey = branch.GetKey(childIndex);
				}

			} // while (true)
		}

		public void DeleteLeaf(Key key) {
			// Set up the state
			StackFrame frame = StackEnd(0);
			NodeId thisRef = frame.NodeId;
			TreeBranch branchNode = null;
			int deleteNodeSize = -1;
			Key leftKey = null;

			// Walk back through the rest of the stack
			int sz = FrameCount;
			for (int i = 1; i < sz; ++i) {
				// Find the child_i for the child
				// This is the child_i of the child in the current branch
				int childIndex = frame.ChildIndex;

				// Move the stack frame,
				frame = StackEnd(i);

				NodeId childRef = thisRef;
				thisRef = frame.NodeId;
				TreeBranch childBranch = branchNode;
				branchNode = (TreeBranch)UnfreezeNode(FetchNode(thisRef));

				if (deleteNodeSize == -1)
					deleteNodeSize = (int)branchNode.GetChildLeafElementCount(childIndex);

				// If the child branch is empty,
				if (childBranch == null || childBranch.IsEmpty) {
					// Delete the reference to it,
					if (childIndex == 0 && branchNode.ChildCount > 1)
						leftKey = branchNode.GetKey(1);

					branchNode.RemoveChild(childIndex);
					// Delete the child branch,
					DeleteNode(childRef);
				}
					// Not empty,
				else {
					// Replace with the new child node reference
					branchNode.SetChild(childIndex, childBranch.Id);
					// Set the element count
					long newChildSize = branchNode.GetChildLeafElementCount(childIndex) - deleteNodeSize;
					branchNode.SetChildLeafElementCount(childIndex, newChildSize);
					// Can we set the left key reference?
					if (childIndex > 0 && leftKey != null) {
						branchNode.SetKeyValueToLeft(leftKey, childIndex);
						leftKey = null;
					}

					// Has the size of the child reached the lower threshold?
					if (childBranch.ChildCount <= 2) {
						// If it has, we need to redistribute the children,
						RedistributeBranchElements(branchNode, childIndex, childBranch);
					}
				}
			}

			// Finally, set the root node
			// If the branch node is a single element, we set the root as the child,
			if (branchNode.ChildCount == 1) {
				// This shrinks the height of the tree,
				RootNodeId = branchNode.GetChild(0);
				DeleteNode(branchNode.Id);
				if (TreeHeight != -1) {
					TreeHeight = TreeHeight - 1;
				}
			} else {
				// Otherwise, we set the branch node.
				RootNodeId = branchNode.Id;
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
			long bytesRemoved = 0;

			while (true) {
				// Set up for the node,
				SetupForPosition(key, p);
				// This is the stopping condition, when the start of the node is
				// before the back_position,
				if (StackEnd(0).Offset <= backPosition)
					return bytesRemoved;

				// The current leaf size
				int leafSize = LeafSize;
				// The bytes removed is the size of the leaf,
				bytesRemoved += LeafSize;
				// Otherwise, delete the leaf
				DeleteLeaf(key);

				p -= leafSize;
			}
		}

		public void SplitLeaf(Key key, long position) {
			UnfreezeStack();
			TreeLeaf sourceLeaf = CurrentLeaf;
			int splitPoint = LeafOffset;
			// The amount of data we are copying from the current key.
			int amount = sourceLeaf.Length - splitPoint;
			// Create a new empty node
			TreeLeaf emptyLeaf = CreateLeaf(key);
			emptyLeaf.SetLength(amount);
			// Copy the data at the end of the leaf into a buffer
			byte[] buf = new byte[amount];
			sourceLeaf.Read(splitPoint, buf, 0, amount);
			// And write it out to the new leaf
			emptyLeaf.Write(0, buf, 0, amount);
			// Set the new size of the node
			sourceLeaf.SetLength(splitPoint);
			// Update the stack properties
			UpdateStackProperties(-amount);
			// And insert the new leaf after
			InsertLeaf(key, emptyLeaf, false);
		}

		public void AddSpaceAfter(Key key, long spaceToAdd) {
			while (spaceToAdd > 0) {
				// Create an empty sparse node
				TreeLeaf emptyLeaf = CreateSparseLeaf(key, 0, spaceToAdd);
				InsertLeaf(key, emptyLeaf, false);
				spaceToAdd -= emptyLeaf.Length;
			}
		}

		public int ExpandLeaf(long amount) {
			if (amount > 0) {
				UnfreezeStack();
				int actualExpandBy = (int)Math.Min((long)currentLeaf.Capacity - currentLeaf.Length, amount);
				if (actualExpandBy > 0) {
					currentLeaf.SetLength(currentLeaf.Length + actualExpandBy);
					UpdateStackProperties(actualExpandBy);
				}
				return actualExpandBy;
			}
			return 0;
		}

		public void TrimAtPosition() {
			UnfreezeStack();
			int sizeBefore = currentLeaf.Length;
			currentLeaf.SetLength(leafOffset);
			UpdateStackProperties(leafOffset - sizeBefore);
		}

		public void ShiftLeaf(long amount) {
			if (amount != 0) {
				UnfreezeStack();
				int sizeBefore = currentLeaf.Length;
				currentLeaf.Shift(leafOffset, (int)amount);
				UpdateStackProperties(currentLeaf.Length - sizeBefore);
			}
		}

		public bool MoveToNextLeaf(Key key) {
			long nextPos = StackEnd(0).Offset + currentLeaf.Length;
			SetupForPosition(key, nextPos);
			return leafOffset != currentLeaf.Length &&
				   currentLeafKey.Equals(key);
		}

		public bool MoveToPreviousLeaf(Key key) {
			long previousPos = StackEnd(0).Offset - 1;
			SetupForPosition(key, previousPos);
			return currentLeafKey.Equals(key);
		}

		public int ReadInto(Key key, long currentPosition, byte[] buf, int off, int len) {
			int read = 0;
			// While there is information to read into the array,
			while (len > 0) {
				// Set up the stack and internal variables for the given position,
				SetupForPosition(key, currentPosition);
				// Read as much as we can from the current leaf capped at the leaf size
				// if necessary,
				int toRead = Math.Min(len, currentLeaf.Length - leafOffset);
				if (toRead == 0)
					throw new ApplicationException("Read out of bounds.");

				// Copy the leaf into the array,
				currentLeaf.Read(leafOffset, buf, off, toRead);
				// Modify the pointers
				currentPosition += toRead;
				off += toRead;
				len -= toRead;
				read += toRead;
			}

			return read;
		}

		public void WriteFrom(Key key, long currentPosition, byte[] buf, int off, int len) {
			// While there is information to read into the array,
			while (len > 0) {
				// Set up the stack and internal variables for the given position,
				SetupForPosition(key, currentPosition);
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
				currentPosition += to_write;
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

		#region StackFrame

		private class StackFrame {

			private readonly long[] stack;
			private readonly int offset;

			public StackFrame(long[] stack, int offset) {
				this.stack = stack;
				this.offset = offset;
			}

			public int ChildIndex {
				get { return (int) stack[offset]; }
			}

			public long Offset {
				get { return stack[offset + 1]; }
			}

			public NodeId NodeId {
				get { return new NodeId(stack[offset + 2], stack[offset + 3]); }
				set {
					stack[offset + 2] = value.High;
					stack[offset + 3] = value.Low;
				}
			}
		}

		#endregion
	}
}