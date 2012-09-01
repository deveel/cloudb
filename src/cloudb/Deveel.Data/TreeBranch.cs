using System;

namespace Deveel.Data {
	public class TreeBranch : ITreeNode {
		private NodeId nodeId;
		private long[] children;
		private int childrenCount;

		public TreeBranch(NodeId nodeId, int maxChildrenCount) {
			if (!nodeId.IsInMemory)
				throw new ArgumentException("Only heap node permitted.");
			if ((maxChildrenCount%2) != 0)
				throw new ArgumentException("max_children_count must be a multiple of 2.");
			if (maxChildrenCount > 65530)
				throw new ArgumentException("Branch children count is limited to 65530");
			if (maxChildrenCount < 6)
				// While I did test with 4, tree balancing is rather tough at 4 so we
				// should have this at at least 6.
				throw new ArgumentException("max_children_count must be >= 6");

			this.nodeId = nodeId;

			children = new long[(maxChildrenCount*5) - 2];
			childrenCount = 0;
		}

		public TreeBranch(NodeId nodeId, TreeBranch branch, int maxChildrenCount)
			: this(nodeId, maxChildrenCount) {
			Array.Copy(branch.children, 0, children, 0, Math.Min(branch.children.Length, children.Length));
			childrenCount = branch.ChildCount;
		}

		public TreeBranch(NodeId nodeId, long[] data, int dataSize) {
			if (nodeId.IsInMemory)
				throw new ArgumentException("Only store nodes permitted.");

			this.nodeId = nodeId;

			children = data;
			childrenCount = (dataSize + 2)/5;
		}

		public void Dispose() {
		}

		public NodeId Id {
			get { return nodeId; }
		}

		public virtual long MemoryAmount {
			get {
				// The size of the member variables + byte estimate for heap use for
				// object maintenance.
				return 8 + 4 + (children.Length*8) + 64;
			}
		}

		public long[] NodeData {
			get { return children; }
		}

		public int NodeDataSize {
			get { return (ChildCount*5) - 2; }
		}

		public long LeafElementCount {
			get {
				int elements = ChildCount;
				long size = 0;
				int p = 0;
				for (; elements > 0; --elements) {
					size += children[p + 2];
					p += 5;
				}
				return size;
			}
		}

		public bool IsReadOnly {
			get { return !nodeId.IsInMemory; }
		}

		public int ChildCount {
			get { return childrenCount; }
		}

		public int MaxChildCount {
			get { return (children.Length + 2)/5; }
		}

		public bool IsFull {
			get { return ChildCount == MaxChildCount; }
		}

		public bool IsEmpty {
			get { return ChildCount == 0; }
		}

		public Key MidPointKey {
			get {
				int n = children.Length/2;
				return new Key(children[n - 1], children[n]);
			}
		}

		private void CheckReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException("The node is read-only.");
		}

		private long InternalGetChildSize(int p) {
			return children[p + 2];
		}

		internal void SetChildLeafElementCount(long count, int childIndex) {
			CheckReadOnly();
			if (childIndex >= ChildCount)
				throw new ArgumentOutOfRangeException("childIndex", "Child request out of bounds.");

			children[(childIndex*5) + 2] = count;
		}

		internal void SetKeyToLeft(Key key, int child_i) {
			CheckReadOnly();
			if (child_i >= ChildCount)
				throw new ArgumentOutOfRangeException("child_i", "Key request out of bounds.");

			children[(child_i*5) - 2] = key.GetEncoded(1);
			children[(child_i*5) - 1] = key.GetEncoded(2);
		}

		public NodeId GetChild(int n) {
			if (n >= ChildCount)
				throw new ArgumentOutOfRangeException("n", "Child request out of bounds.");

			int p = (n*5);
			return new NodeId(children[p], children[p + 1]);
		}

		public Key GetKey(int n) {
			if (n >= ChildCount)
				throw new ArgumentOutOfRangeException("n", "Key request out of bounds.");

			long keyV1 = children[(n*5) - 2];
			long keyV2 = children[(n*5) - 1];
			return new Key(keyV1, keyV2);
		}

		public void MoveLastHalfInto(TreeBranch dest) {
			int midpoint = children.Length/2;

			// Check mutable
			CheckReadOnly();
			dest.CheckReadOnly();

			// Check this is full
			if (!IsFull)
				throw new InvalidOperationException("Branch node is not full.");

			// Check destination is empty
			if (!dest.IsEmpty)
				throw new ArgumentException("Destination branch node is not empty.");

			// Copy,
			Array.Copy(children, midpoint + 1, dest.children, 0, midpoint - 1);

			// New child count in each branch node.
			int newChildCount = MaxChildCount/2;

			// Set the size of this and the destination node
			childrenCount = newChildCount;
			dest.childrenCount = newChildCount;
		}

		public Key MergeLeft(TreeBranch right, Key midValue, int count) {
			// Check mutable
			CheckReadOnly();

			// If we moving all from right,
			if (count == right.ChildCount) {
				// Move all the elements into this node,
				int destP = childrenCount*5;
				int rightLen = (right.childrenCount*5) - 2;
				Array.Copy(right.children, 0, children, destP, rightLen);
				children[destP - 2] = midValue.GetEncoded(1);
				children[destP - 1] = midValue.GetEncoded(2);
				// Update children_count
				childrenCount += right.childrenCount;
				return null;
			}
			if (count < right.ChildCount) {
				right.CheckReadOnly();

				// Shift elements from right to left
				// The amount to move that will leave the right node at min threshold
				int destP = ChildCount*5;
				int rightLen = (count*5) - 2;
				Array.Copy(right.children, 0, children, destP, rightLen);
				// Redistribute the right elements
				int rightRedist = (count*5);
				// The midpoint value becomes the extent shifted off the end
				long newMidpointValue1 = right.children[rightRedist - 2];
				long newMidpointValue2 = right.children[rightRedist - 1];
				// Shift the right child
				Array.Copy(right.children, rightRedist, right.children, 0,
				           right.children.Length - rightRedist);
				children[destP - 2] = midValue.GetEncoded(1);
				children[destP - 1] = midValue.GetEncoded(2);
				childrenCount += count;
				right.childrenCount -= count;

				// Return the new midpoint value
				return new Key(newMidpointValue1, newMidpointValue2);
			}

			throw new ApplicationException("count > right.size()");
		}

		public Key Merge(TreeBranch right, Key midValue) {
			// Check mutable
			CheckReadOnly();
			right.CheckReadOnly();

			// How many elements in total?
			int totalElements = ChildCount + right.ChildCount;
			// If total elements is smaller than max size,
			if (totalElements <= MaxChildCount) {
				// Move all the elements into this node,
				int destP = childrenCount*5;
				int rightLen = (right.childrenCount*5) - 2;
				Array.Copy(right.children, 0, children, destP, rightLen);
				children[destP - 2] = midValue.GetEncoded(1);
				children[destP - 1] = midValue.GetEncoded(2);
				// Update children_count
				childrenCount += right.childrenCount;
				right.childrenCount = 0;

				return null;
			} else {
				long newMidpointValue1, newMidpointValue2;

				// Otherwise distribute between the nodes,
				int maxShift = (MaxChildCount + right.MaxChildCount) - totalElements;
				if (maxShift <= 2)
					return midValue;

				int minThreshold = MaxChildCount/2;
				if (ChildCount < right.ChildCount) {
					// Shift elements from right to left
					// The amount to move that will leave the right node at min threshold
					int count = Math.Min(right.ChildCount - minThreshold, maxShift);
					int destP = ChildCount*5;
					int rightLen = (count*5) - 2;

					Array.Copy(right.children, 0, children, destP, rightLen);
					// Redistribute the right elements
					int rightRedist = (count*5);
					// The midpoint value becomes the extent shifted off the end
					newMidpointValue1 = right.children[rightRedist - 2];
					newMidpointValue2 = right.children[rightRedist - 1];
					// Shift the right child
					Array.Copy(right.children, rightRedist, right.children, 0,
					           right.children.Length - rightRedist);
					children[destP - 2] = midValue.GetEncoded(1);
					children[destP - 1] = midValue.GetEncoded(2);
					childrenCount += count;
					right.childrenCount -= count;

				} else {
					// Shift elements from left to right
					// The amount to move that will leave the left node at min threshold
					int count = Math.Min(ChildCount - minThreshold, maxShift);

					// Make room for these elements
					int rightRedist = (count*5);
					Array.Copy(right.children, 0, right.children, rightRedist,
					           right.children.Length - rightRedist);
					int srcP = (ChildCount - count)*5;
					int leftLen = (count*5) - 2;
					Array.Copy(children, srcP, right.children, 0, leftLen);
					right.children[rightRedist - 2] = midValue.GetEncoded(1);
					right.children[rightRedist - 1] = midValue.GetEncoded(2);
					// The midpoint value becomes the extent shifted off the end
					newMidpointValue1 = children[srcP - 2];
					newMidpointValue2 = children[srcP - 1];
					// Update children counts
					childrenCount -= count;
					right.childrenCount += count;
				}

				return new Key(newMidpointValue1, newMidpointValue2);
			}
		}

		public void SetChildOverride(NodeId childPointer, int n) {
			children[(n*5) + 0] = childPointer.High;
			children[(n*5) + 1] = childPointer.Low;
		}

		internal void SetChild(NodeId child_pointer, int n) {
			CheckReadOnly();
			SetChildOverride(child_pointer, n);
		}

		public void Set(NodeId child1, long child1Count, Key key, NodeId child2, long child2Count) {
			CheckReadOnly();
			// Set the values
			children[0] = child1.High;
			children[1] = child1.Low;
			children[2] = child1Count;
			children[3] = key.GetEncoded(1);
			children[4] = key.GetEncoded(2);
			children[5] = child2.High;
			children[6] = child2.Low;
			children[7] = child2Count;
			// Increase the child count.
			childrenCount += 2;
		}

		public void Insert(NodeId child1, long child1Count, Key key, NodeId child2, long child2Count, int n) {
			CheckReadOnly();
			// Shift the array by 5
			int p1 = (n*5) + 3;
			int p2 = (n*5) + 8;
			Array.Copy(children, p1, children, p2, children.Length - p2);
			// Insert the values
			children[p1 - 3] = child1.High;
			children[p1 - 2] = child1.Low;
			children[p1 - 1] = child1Count;
			children[p1 + 0] = key.GetEncoded(1);
			children[p1 + 1] = key.GetEncoded(2);
			children[p1 + 2] = child2.High;
			children[p1 + 3] = child2.Low;
			children[p1 + 4] = child2Count;
			// Increase the child count.
			++childrenCount;
		}

		internal void RemoveChild(int childIndex) {
			CheckReadOnly();
			if (childIndex == 0) {
				Array.Copy(children, 5, children, 0, children.Length - 5);
			} else if (childIndex + 1 < childrenCount) {
				int p1 = (childIndex*5) + 3;
				Array.Copy(children, p1, children, p1 - 5, children.Length - p1);
			}
			--childrenCount;
		}


		public long GetChildLeafElementCount(int childIndex) {
			return InternalGetChildSize(childIndex*5); // children[(child_i * 5) + 2];
		}


		public long GetChildOffset(int childIndex) {
			long offset = 0;
			int p = 0;
			for (; childIndex > 0; --childIndex) {
				offset += InternalGetChildSize(p); //children[p + 2];
				p += 5;
			}
			return offset;
		}

		public int IndexOfChild(Key key, long offset) {

			if (offset >= 0) {
				int sz = ChildCount;
				long leftOffset = 0;
				for (int i = 0; i < sz; ++i) {
					leftOffset += GetChildLeafElementCount(i);
					// If the relative point must be within this child
					if (offset < leftOffset)
						return i;

					// This is a boundary condition, we need to use the key to work out
					// which child to take
					if (offset == leftOffset) {
						// If the end has been reached,
						if (i == sz - 1)
							return i;

						Key keyVal = GetKey(i + 1);
						int n = keyVal.CompareTo(key);
						// If the key being inserted is less than the new leaf node,
						if (n > 0)
							// Go left,
							return i;

						// Otherwise go right
						return i + 1;
					}
				}
			}

			return -1;
		}

		public int SearchFirst(Key key) {
			int low = 1;
			int high = ChildCount - 1;

			while (true) {

				if (high - low <= 2) {
					for (int i = low; i <= high; ++i) {
						int cmp = GetKey(i).CompareTo(key);
						if (cmp > 0) {
							// Value is less than extent so take the left route
							return i - 1;
						}
						if (cmp == 0) {
							// Equal so need to search left and right of the extent
							// This is (-(i + 1 - 1))
							return -i;
						}
					}
					// Value is greater than extent so take the right route
					return high;
				}

				{
					int mid = (low + high)/2;
					int cmp = GetKey(mid).CompareTo(key);

					if (cmp < 0) {
						low = mid + 1;
					} else if (cmp > 0) {
						high = mid - 1;
					} else {
						high = mid;
					}
				}
			}
		}

		public int SearchLast(Key key) {

			int low = 1;
			int high = ChildCount - 1;

			while (true) {

				if (high - low <= 2) {
					for (int i = high; i >= low; --i) {
						int cmp = GetKey(i).CompareTo(key);
						if (cmp <= 0) {
							return i;
						}
					}
					return low - 1;
				}

				{
					int mid = (low + high)/2;
					int cmp = GetKey(mid).CompareTo(key);

					if (cmp < 0) {
						low = mid + 1;
					} else if (cmp > 0) {
						high = mid - 1;
					} else {
						low = mid;
					}
				}

			}
		}
	}
}