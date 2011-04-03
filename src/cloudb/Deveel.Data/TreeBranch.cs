using System;

namespace Deveel.Data {
	public class TreeBranch : ITreeNode {
		private readonly NodeId id;
		private int childCount;
		private readonly long[] children;

		public TreeBranch(NodeId id, int maxChildCount) {
			if (!id.IsInMemory)
				throw new ArgumentException("Only heap node permitted.", "id");
			if ((maxChildCount % 2) != 0)
				throw new ArgumentException("The number of maximum children must be a multiple of 2.", "maxChildCount");
			if (maxChildCount > 65530)
				throw new ArgumentException("Branch children count is limited to 65530", "maxChildCount");
			if (maxChildCount < 6)
				throw new ArgumentException("The number of maximum children must be greater or equal to 6.", "maxChildCount");

			this.id = id;
			
			children = new long[(maxChildCount * 5) - 2];
			childCount = 0;
		}

		public TreeBranch(NodeId id, TreeBranch branch, int maxChildCount)
			: this(id, maxChildCount) {
			Array.Copy(branch.children, 0, children, 0, Math.Min(branch.children.Length, children.Length));
			childCount = branch.childCount;
		}

		public TreeBranch(NodeId id, long[] children, int node_data_size) {
			if (id.IsInMemory)
				throw new ArgumentException("Only store nodes permitted.");

			this.id = id;
			this.children = children;
			childCount = (node_data_size + 2) / 5;
		}

		public int ChildCount {
			get { return childCount; }
		}

		public int MaxSize {
			get { return (children.Length + 2)/5; }
		}

		public bool IsReadOnly {
			get { return !id.IsInMemory; }
		}

		public bool IsEmpty {
			get { return (childCount == 0); }
		}

		public bool IsFull {
			get { return ChildCount == MaxSize; }
		}

		public bool IsLessThanHalfFull {
			get { return (ChildCount < (MaxSize/2)); }
		}

		public Key MidPointKey {
			get {
				int n = children.Length / 2;
				return new Key(children[n - 1], children[n]);
			}
		}

		internal long [] ChildPointers {
			get { return children; }
		}

		public int DataSize {
			get { return (childCount*5) - 2; }
		}

		private void CheckReadOnly() {
			if (IsReadOnly)
				throw new ApplicationException("Node is read-only.");
		}

		private long InternalGetChildSize(int p) {
			return children[p + 2];
		}

		#region Implementation of IDisposable

		public void Dispose() {
		}

		#endregion

		#region Implementation of ITreeNode

		public NodeId Id {
			get { return id; }
		}

		public virtual long MemoryAmount {
			get { return 8 + 4 + (children.Length * 8) + 64; }
		}

		public long LeafElementCount {
			get {
				int elements = childCount;
				long size = 0;
				int p = 0;
				for (; elements > 0; --elements) {
					size += children[p + 2];
					p += 5;
				}
				return size;
			}
		}

		#endregion

		internal void SetKeyValueToLeft(KeyBase key, int child_i) {
			if (child_i >= ChildCount)
				throw new ArgumentOutOfRangeException("child_i", "Key request out of bounds.");

			children[(child_i * 5) - 2] = key.GetEncoded(1);
			children[(child_i * 5) - 1] = key.GetEncoded(2);
		}

		//internal void SetKeyValueToLeft(Key k, int child_i) {
		//    SetKeyValueToLeft(k.GetEncoded(1), k.GetEncoded(2), child_i);
		//}

		internal void SetChildOverride(int index, NodeId value) {
			children[(index * 5) + 0] = value.High;
			children[(index * 5) + 1] = value.Low;
		}

		internal void SetChildLeafElementCount(int childIndex, long count) {
			CheckReadOnly();
			if (childIndex >= ChildCount)
				throw new ArgumentOutOfRangeException("childIndex", "Child request out of bounds.");
			children[(childIndex * 5) + 2] = count;
		}

		internal void RemoveChild(int index) {
			CheckReadOnly();
			if (index == 0) {
				Array.Copy(children, 5, children, 0, children.Length - 5);
			} else if (index + 1 < childCount) {
				int p1 = (index * 5) + 3;
				Array.Copy(children, p1, children, p1 - 5, children.Length - p1);
			}
			--childCount;
		}

		public NodeId GetChild(int index) {
			if (index >= ChildCount)
				throw new ArgumentOutOfRangeException("index", "Child request out of bounds.");

			int p = (index*5);
			return new NodeId(children[p], children[p + 1]);
		}

		public void SetChild(int index, NodeId value) {
			CheckReadOnly();
			SetChildOverride(index, value);
		}

		public Key GetKey(int index) {
			if (index >= ChildCount)
				throw new ArgumentOutOfRangeException("index", "Key request out of bounds.");

			long v1 = children[(index * 5) - 2];
			long v2 = children[(index * 5) - 1];
			return new Key(v1, v2);
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
			childCount += 2;
		}

		public void Insert(NodeId child1, long child1Count, Key key, NodeId child2, long child2Count, int index) {
			CheckReadOnly();
			// Shift the array by 5
			int p1 = (index * 5) + 3;
			int p2 = (index * 5) + 8;
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
			++childCount;
		}

		public int SearchFirst(Key key) {
			int low = 1;
			int high = ChildCount - 1;

			while (true) {
				if (high - low <= 2) {
					for (int i = low; i <= high; ++i) {
						int cmp1 = GetKey(i).CompareTo(key);
						if (cmp1 > 0)
							// Value is less than extent so take the left route
							return i - 1;
						if (cmp1 == 0)
							// Equal so need to search left and right of the extent
							// This is (-(i + 1 - 1))
							return -i;
					}
					// Value is greater than extent so take the right route
					return high;
				}

				int mid = (low + high) / 2;
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

		public int SearchLast(Key key) {
			int low = 1;
			int high = ChildCount - 1;

			while (true) {

				if (high - low <= 2) {
					for (int i = high; i >= low; --i) {
						int cmp1 = GetKey(i).CompareTo(key);
						if (cmp1 <= 0)
							return i;
					}
					return low - 1;
				}

				int mid = (low + high) / 2;
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

		public int IndexOfChild(Key key, long offset) {
			if (offset >= 0) {
				int sz = ChildCount;
				long left_offset = 0;
				for (int i = 0; i < sz; ++i) {
					left_offset += GetChildLeafElementCount(i);
					// If the relative point must be within this child
					if (offset < left_offset)
						return i;

					// This is a boundary condition, we need to use the key to work out
					// which child to take
					if (offset == left_offset) {
						// If the end has been reached,
						if (i == sz - 1)
							return i;

						Key key_val = GetKey(i + 1);
						int n = key_val.CompareTo(key);
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

		public int IndexOfChild(long reference) {
			int sz = ChildCount;
			for (int i = 0; i < sz; ++i) {
				if (children[(i * 4)] == reference)
					return i;
			}
			return -1;
		}

		public long GetChildOffset(int index) {
			long offset = 0;
			int p = 0;
			for (; index > 0; --index) {
				offset += InternalGetChildSize(p);
				p += 5;
			}
			return offset;
		}

		public long GetChildLeafElementCount(int index) {
			return InternalGetChildSize(index * 5);
		}

		public int GetSibling(int index) {
			if (index == 0)
				return 1;
			return index - 1;
		}

		public Key MergeLeft(TreeBranch right, Key midValue, int count) {
			// Check mutable
			CheckReadOnly();

			// If we moving all from right,
			if (count == right.ChildCount) {
				// Move all the elements into this node,
				int destPoint = childCount * 5;
				int rightLength = (right.childCount * 5) - 2;
				Array.Copy(right.children, 0, children, destPoint, rightLength);
				children[destPoint - 2] = midValue.GetEncoded(1);
				children[destPoint - 1] = midValue.GetEncoded(2);
				// Update children_count
				childCount += right.childCount;

				return null;
			} 
			if (count < right.ChildCount) {
				right.CheckReadOnly();

				// Shift elements from right to left
				// The amount to move that will leave the right node at min threshold
				int destPoint = ChildCount * 5;
				int rightLength = (count * 5) - 2;
				Array.Copy(right.children, 0, children, destPoint, rightLength);
				// Redistribute the right elements
				int rightRedist = (count * 5);
				// The midpoint value becomes the extent shifted off the end
				long newMidpointValue1 = right.children[rightRedist - 2];
				long newMidpointValue2 = right.children[rightRedist - 1];
				// Shift the right child
				Array.Copy(right.children, rightRedist, right.children, 0,
								 right.children.Length - rightRedist);
				children[destPoint - 2] = midValue.GetEncoded(1);
				children[destPoint - 1] = midValue.GetEncoded(2);
				childCount += count;
				right.childCount -= count;

				// Return the new midpoint value
				return new Key(newMidpointValue1, newMidpointValue2);
			}
			
			throw new ArgumentException("count > right.size()");
		}

		public Key Merge(TreeBranch right, Key midValue) {
			CheckReadOnly();
			right.CheckReadOnly();

			// How many elements in total?
			int totalElements = ChildCount + right.ChildCount;
			// If total elements is smaller than max size,
			if (totalElements <= MaxSize) {
				// Move all the elements into this node,
				int destPoint = childCount * 5;
				int rightLength = (right.childCount * 5) - 2;
				Array.Copy(right.children, 0, children, destPoint, rightLength);
				children[destPoint - 2] = midValue.GetEncoded(1);
				children[destPoint - 1] = midValue.GetEncoded(2);
				// Update children_count
				childCount += right.childCount;
				right.childCount = 0;
				return null;
			} else {
				long newMidpointValue1, newMidpointValue2;

				// Otherwise distribute between the nodes,
				int maxShift = (MaxSize + right.MaxSize) - totalElements;
				if (maxShift <= 2) {
					return midValue;
				}
				int minThreshold = MaxSize / 2;
				//      int half_total_elements = total_elements / 2;
				if (ChildCount < right.ChildCount) {
					// Shift elements from right to left
					// The amount to move that will leave the right node at min threshold
					int count = Math.Min(right.ChildCount - minThreshold, maxShift);
					int destPoint = ChildCount * 5;
					int right_len = (count * 5) - 2;
					Array.Copy(right.children, 0, children, destPoint, right_len);
					// Redistribute the right elements
					int right_redist = (count * 5);
					// The midpoint value becomes the extent shifted off the end
					newMidpointValue1 = right.children[right_redist - 2];
					newMidpointValue2 = right.children[right_redist - 1];
					// Shift the right child
					Array.Copy(right.children, right_redist, right.children, 0,
									 right.children.Length - right_redist);
					children[destPoint - 2] = midValue.GetEncoded(1);
					children[destPoint - 1] = midValue.GetEncoded(2);
					childCount += count;
					right.childCount -= count;

				} else {
					// Shift elements from left to right
					// The amount to move that will leave the left node at min threshold
					int count = Math.Min(MaxSize - minThreshold, maxShift);
					//        int count = Math.Min(half_total_elements - right.ChildCount, max_shift);

					// Make room for these elements
					int rightRedist = (count * 5);
					Array.Copy(right.children, 0, right.children, rightRedist,
									 right.children.Length - rightRedist);
					int srcPoint = (ChildCount - count) * 5;
					int leftLength = (count * 5) - 2;
					Array.Copy(children, srcPoint, right.children, 0, leftLength);
					right.children[rightRedist - 2] = midValue.GetEncoded(1);
					right.children[rightRedist - 1] = midValue.GetEncoded(2);
					// The midpoint value becomes the extent shifted off the end
					newMidpointValue1 = children[srcPoint - 2];
					newMidpointValue2 = children[srcPoint - 1];
					// Update children counts
					childCount -= count;
					right.childCount += count;
				}

				return new Key(newMidpointValue1, newMidpointValue2);
			}
		}

		public void MoveLastHalfInto(TreeBranch dest) {
			int midpoint = children.Length / 2;

			CheckReadOnly();
			dest.CheckReadOnly();

			// Check this is full
			if (!IsFull)
				throw new ApplicationException("Branch node is not full.");

			// Check destination is empty
			if (dest.ChildCount != 0)
				throw new ApplicationException("Destination branch node is not empty.");

			// Copy,
			Array.Copy(children, midpoint + 1, dest.children, 0, midpoint - 1);

			// New child count in each branch node.
			int new_child_count = MaxSize / 2;
			// Set the size of this and the destination node
			childCount = new_child_count;
			dest.childCount = new_child_count;
		}
	}
}