using System;

namespace Deveel.Data.Store {
	public class TreeBranch : ITreeNode {
		private readonly long id;
		private int childCount;
		private readonly long[] children;

		public TreeBranch(long id, int maxChildCount) {
			if (id >= 0)
				throw new ArgumentException("Only heap node permitted.", "id");
			if ((maxChildCount % 2) != 0)
				throw new ArgumentException("The number of maximum children must be a multiple of 2.", "maxChildCount");
			if (maxChildCount > 65530)
				throw new ArgumentException("Branch children count is limited to 65530", "maxChildCount");
			if (maxChildCount < 6)
				throw new ArgumentException("The number of maximum children must be greater or equal to 6.", "maxChildCount");

			this.id = id;
			// f(1) = 2, f(2) = 6, f(3) = 10, f(4) = 14, f(5) = 18
			children = new long[(maxChildCount * 4) - 2];
			childCount = 0;
		}

		public TreeBranch(long id, TreeBranch branch, int maxChildCount)
			: this(id, maxChildCount) {
			Array.Copy(branch.children, 0, children, 0, Math.Min(branch.children.Length, children.Length));
			childCount = branch.childCount;
		}

		public TreeBranch(long id, long[] children, int node_data_size) {
			if (id < 0)
				throw new ArgumentException("id < 0.  Only store nodes permitted.");

			this.id = id;
			this.children = children;
			childCount = (node_data_size + 2) / 4;
		}

		public int ChildCount {
			get { return childCount; }
		}

		public int MaxSize {
			get { return (children.Length + 2)/4; }
		}

		public bool IsReadOnly {
			get { return id > 0; }
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
			get { return (childCount*4) - 2; }
		}

		private void CheckReadOnly() {
			if (IsReadOnly)
				throw new ApplicationException("Node is read-only.");
		}

		#region Implementation of IDisposable

		public void Dispose() {
		}

		#endregion

		#region Implementation of ITreeNode

		public long Id {
			get { return id; }
		}

		public virtual long MemoryAmount {
			get { return 8 + 4 + (children.Length * 8) + 64; }
		}

		public long LeafElementCount {
			get {
				// Add up all the elements of the children
				long leaf_element_count = 0;
				int end = (childCount*4) - 2;
				for (int i = 1; i < end; i += 4)
					leaf_element_count += children[i];
				return leaf_element_count;
			}
		}

		#endregion

		internal void SetKeyValueToLeft(long key_v1, long key_v2, int child_i) {
			CheckReadOnly();
			children[(child_i * 4) - 2] = key_v1;
			children[(child_i * 4) - 1] = key_v2;
		}

		internal void SetKeyValueToLeft(Key k, int child_i) {
			SetKeyValueToLeft(k.GetEncoded(1), k.GetEncoded(2), child_i);
		}

		internal void SetChildOverride(int index, long value) {
			children[index * 4] = value;
		}

		internal void SetChildLeafElementCount(int childIndex, long count) {
			CheckReadOnly();
			children[(childIndex * 4) + 1] = count;
		}

		internal void RemoveChild(int index) {
			CheckReadOnly();
			if (index == 0) {
				Array.Copy(children, 4, children, 0, children.Length - 4);
			} else if (index + 1 < childCount) {
				int p1 = (index * 4) + 2;
				Array.Copy(children, p1, children, p1 - 4, children.Length - p1);
			}
			--childCount;
		}

		public long GetChild(int index) {
			return children[index * 4];
		}

		public void SetChild(int index, long value) {
			CheckReadOnly();
			SetChildOverride(index, value);
		}

		public Key GetKey(int index) {
			long v1 = children[(index * 4) - 2];
			long v2 = children[(index * 4) - 1];
			return new Key(v1, v2);
		}

		public void Set(long child1, long child1_count, long key1,
						long key2, long child2, long child2_count) {
			CheckReadOnly();

			// Set the values
			children[0] = child1;
			children[1] = child1_count;
			children[2] = key1;
			children[3] = key2;
			children[4] = child2;
			children[5] = child2_count;
			// Increase the child count.
			childCount += 2;
		}

		public void Insert(long child1, long child1_count, long key1, 
						   long key2, long child2, long child2_count, int n) {
			CheckReadOnly();
			// Shift the array by 4
			int p1 = (n * 4) + 2;
			int p2 = (n * 4) + 6;
			Array.Copy(children, p1, children, p2, children.Length - p2);
			// Insert the values
			children[p1 - 2] = child1;
			children[p1 - 1] = child1_count;
			children[p1 + 0] = key1;
			children[p1 + 1] = key2;
			children[p1 + 2] = child2;
			children[p1 + 3] = child2_count;
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
			for (int i = 0; i < index; ++i) {
				offset += children[(i * 4) + 1];
			}
			return offset;
		}

		public long GetChildLeafElementCount(int index) {
			return children[(index * 4) + 1];
		}

		public int GetSibling(int index) {
			if (index == 0)
				return 1;
			return index - 1;
		}

		public Key MergeLeft(TreeBranch right, Key mid_value, int count) {
			// Check mutable
			CheckReadOnly();

			// If we moving all from right,
			if (count == right.ChildCount) {
				// Move all the elements into this node,
				int dest_p = childCount * 4;
				int right_len = (right.childCount * 4) - 2;
				Array.Copy(right.children, 0, children, dest_p, right_len);
				children[dest_p - 2] = mid_value.GetEncoded(1);
				children[dest_p - 1] = mid_value.GetEncoded(2);
				// Update children_count
				childCount += right.childCount;

				return null;
			} 
			if (count < right.ChildCount) {
				right.CheckReadOnly();

				// Shift elements from right to left
				// The amount to move that will leave the right node at min threshold
				int dest_p = ChildCount * 4;
				int right_len = (count * 4) - 2;
				Array.Copy(right.children, 0, children, dest_p, right_len);
				// Redistribute the right elements
				int right_redist = (count * 4);
				// The midpoint value becomes the extent shifted off the end
				long new_midpoint_value1 = right.children[right_redist - 2];
				long new_midpoint_value2 = right.children[right_redist - 1];
				// Shift the right child
				Array.Copy(right.children, right_redist, right.children, 0,
								 right.children.Length - right_redist);
				children[dest_p - 2] = mid_value.GetEncoded(1);
				children[dest_p - 1] = mid_value.GetEncoded(2);
				childCount += count;
				right.childCount -= count;

				// Return the new midpoint value
				return new Key(new_midpoint_value1, new_midpoint_value2);
			}
			
			throw new ArgumentException("count > right.size()");
		}

		public Key Merge(TreeBranch right, Key midValue) {
			CheckReadOnly();
			right.CheckReadOnly();

			// How many elements in total?
			int total_elements = ChildCount + right.ChildCount;
			// If total elements is smaller than max size,
			if (total_elements <= MaxSize) {
				// Move all the elements into this node,
				int dest_p = childCount * 4;
				int right_len = (right.childCount * 4) - 2;
				Array.Copy(right.children, 0, children, dest_p, right_len);
				children[dest_p - 2] = midValue.GetEncoded(1);
				children[dest_p - 1] = midValue.GetEncoded(2);
				// Update children_count
				childCount += right.childCount;
				right.childCount = 0;
				return null;
			} else {
				long new_midpoint_value1, new_midpoint_value2;

				// Otherwise distribute between the nodes,
				int max_shift = (MaxSize + right.MaxSize) - total_elements;
				if (max_shift <= 2) {
					return midValue;
				}
				int min_threshold = MaxSize / 2;
				//      final int half_total_elements = total_elements / 2;
				if (ChildCount < right.ChildCount) {
					// Shift elements from right to left
					// The amount to move that will leave the right node at min threshold
					int count = Math.Min(right.ChildCount - min_threshold, max_shift);
					int dest_p = ChildCount * 4;
					int right_len = (count * 4) - 2;
					Array.Copy(right.children, 0, children, dest_p, right_len);
					// Redistribute the right elements
					int right_redist = (count * 4);
					// The midpoint value becomes the extent shifted off the end
					new_midpoint_value1 = right.children[right_redist - 2];
					new_midpoint_value2 = right.children[right_redist - 1];
					// Shift the right child
					Array.Copy(right.children, right_redist, right.children, 0,
									 right.children.Length - right_redist);
					children[dest_p - 2] = midValue.GetEncoded(1);
					children[dest_p - 1] = midValue.GetEncoded(2);
					childCount += count;
					right.childCount -= count;

				} else {
					// Shift elements from left to right
					// The amount to move that will leave the left node at min threshold
					int count = Math.Min(MaxSize - min_threshold, max_shift);
					//        int count = Math.min(half_total_elements - right.size(), max_shift);

					// Make room for these elements
					int right_redist = (count * 4);
					Array.Copy(right.children, 0, right.children, right_redist,
									 right.children.Length - right_redist);
					int src_p = (ChildCount - count) * 4;
					int left_len = (count * 4) - 2;
					Array.Copy(children, src_p, right.children, 0, left_len);
					right.children[right_redist - 2] = midValue.GetEncoded(1);
					right.children[right_redist - 1] = midValue.GetEncoded(2);
					// The midpoint value becomes the extent shifted off the end
					new_midpoint_value1 = children[src_p - 2];
					new_midpoint_value2 = children[src_p - 1];
					// Update children counts
					childCount -= count;
					right.childCount += count;
				}

				return new Key(new_midpoint_value1, new_midpoint_value2);
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