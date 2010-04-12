using System;
using System.Collections.Generic;

namespace Deveel.Data.Store {
	public sealed class TreeNodeHeap {
		private long nodeIdSeq;
		private readonly IHashNode[] hash;
		private IHashNode hashEnd;
		private IHashNode hashStart;

		private int totalBranchNodeCount;
		private int totalLeafNodeCount;
		private long memoryUsed;

		private long maxMemoryLimit;

		public TreeNodeHeap(int hashSize, long maxMemoryLimit) {
			hash = new IHashNode[hashSize];
			nodeIdSeq = 2;
			this.maxMemoryLimit = maxMemoryLimit;
		}

		private int CalcHashValue(long p) {
			int pp = ((int)-p & 0x0FFFFFFF);
			return pp % hash.Length;
		}

		private void HashNode(IHashNode node) {
			int hash_index = CalcHashValue(node.Id);
			IHashNode old_node = hash[hash_index];
			hash[hash_index] = node;
			node.NextHash = old_node;

			// Add it to the start of the linked list,
			if (hashStart != null) {
				hashStart.Previous = node;
			} else {
				hashEnd = node;
			}
			node.Next = hashStart;
			node.Previous = null;
			hashStart = node;

			// Update the 'memory_used' variable
			memoryUsed += node.MemoryAmount;
			if (node is TreeBranch) {
				++totalBranchNodeCount;
			} else {
				++totalLeafNodeCount;
			}
		}

		private long NextNodeId() {
			long p = nodeIdSeq;
			++nodeIdSeq;
			nodeIdSeq = nodeIdSeq & 0x0FFFFFFFFFFFFFFFL;
			return -p;
		}

		internal void Flush() {
			List<IHashNode> to_flush = null;
			int all_node_count = 0;
			// If the memory use is above some limit then we need to flush out some
			// of the nodes,
			if (memoryUsed > maxMemoryLimit) {
				all_node_count = totalBranchNodeCount + totalLeafNodeCount;
				// The number to clean,
				int to_clean = (int)(all_node_count * 0.30);

				// Make an array of all nodes to flush,
				to_flush = new List<IHashNode>(to_clean);
				// Pull them from the back of the list,
				IHashNode node = hashEnd;
				while (to_clean > 0 && node != null) {
					to_flush.Add(node);
					node = node.Previous;
					--to_clean;
				}
			}

			// If there are nodes to flush,
			if (to_flush != null) {
				// Read each group and call the node flush routine,
				//      System.out.println("We have " + to_flush.size() + " nodes to flush!");

				// The mapping of transaction to node list
				Dictionary<ITransaction, List<long>> tran_map = new Dictionary<ITransaction, List<long>>();
				// Read the list backwards,
				for (int i = to_flush.Count - 1; i >= 0; --i) {
					IHashNode node = to_flush[i];
					// Get the transaction of this node,
					ITransaction tran = node.Transaction;
					// Find the list of this transaction,
					List<long> node_list = tran_map[tran];
					if (node_list == null) {
						node_list = new List<long>(to_flush.Count);
						tran_map.Add(tran, node_list);
					}
					// Add to the list
					node_list.Add(node.Id);
				}
				// Now read the key and dispatch the clean up to the transaction objects,
				foreach(KeyValuePair<ITransaction, List<long>> pair in tran_map) {
					ITransaction tran = pair.Key;
					List<long> node_list = pair.Value;
					// Convert to a 'long[]' array,
					int sz = node_list.Count;
					long[] refs = new long[sz];
					for (int i = 0; i < sz; ++i) {
						refs[i] = node_list[i];
					}
					// Sort the references,
					Array.Sort(refs);
					// Tell the transaction to clean up these nodes,
					((TreeSystemTransaction)tran).FlushNodes(refs);
				}

			}
		}

		public ITreeNode FetchNode(long id) {
			// Fetches the node out of the heap hash array.
			int hashIndex = CalcHashValue(id);
			IHashNode hash_node = hash[hashIndex];
			while (hash_node != null &&
				   hash_node.Id != id) {
				hash_node = hash_node.NextHash;
			}

			return hash_node;
		}

		public TreeBranch CreateBranch(ITransaction tran, int maxBranchChildren) {
			long p = NextNodeId();
			HeapTreeBranch node = new HeapTreeBranch(tran, p, maxBranchChildren);
			HashNode(node);
			return node;
		}

		public TreeLeaf CreateLeaf(ITransaction tran, Key key, int maxLeafSize) {
			long p = NextNodeId();
			HeapTreeLeaf node = new HeapTreeLeaf(tran, p, maxLeafSize);
			HashNode(node);
			return node;
		}

		public ITreeNode Copy(ITreeNode nodeToCopy, int maxBranchSize, int maxLeafSize, ITransaction tran, bool locked) {
			// Create a new pointer for the copy
			long p = NextNodeId();
			if (locked) {
				p = (long)((ulong)p & 0x0DFFFFFFFFFFFFFFFL);
			}
			IHashNode node;
			if (nodeToCopy is TreeLeaf) {
				node = new HeapTreeLeaf(tran, p, (TreeLeaf)nodeToCopy, maxLeafSize);
			} else {
				node = new HeapTreeBranch(tran, p, (TreeBranch)nodeToCopy, maxBranchSize);
			}

			HashNode(node);
			// Return pointer to node
			return node;
		}

		public void Delete(long id) {
			int hash_index = CalcHashValue(id);
			IHashNode hash_node = hash[hash_index];
			IHashNode previous = null;
			while (hash_node != null &&
			       hash_node.Id != id) {
				previous = hash_node;
				hash_node = hash_node.NextHash;
			}
			if (hash_node == null) {
				throw new ApplicationException("Node not found!");
			}
			if (previous == null) {
				hash[hash_index] = hash_node.NextHash;
			} else {
				previous.NextHash = hash_node.NextHash;
			}

			// Remove from the double linked list structure,
			// If removed node at head.
			if (hashStart == hash_node) {
				hashStart = hash_node.Next;
				if (hashStart != null) {
					hashStart.Previous = null;
				} else {
					hashEnd = null;
				}
			}
				// If removed node at end.
			else if (hashEnd == hash_node) {
				hashEnd = hash_node.Previous;
				if (hashEnd != null) {
					hashEnd.Next = null;
				} else {
					hashStart = null;
				}
			} else {
				hash_node.Previous.Next = hash_node.Next;
				hash_node.Next.Previous = hash_node.Previous;
			}

			// Update the 'memory_used' variable
			memoryUsed -= hash_node.MemoryAmount;
			if (hash_node is TreeBranch) {
				--totalBranchNodeCount;
			} else {
				--totalLeafNodeCount;
			}
		}

		private interface IHashNode : ITreeNode {
			IHashNode NextHash { get; set; }

			IHashNode Previous { get; set; }
			IHashNode Next { get; set; }

			ITransaction Transaction { get; }
		}

		private class HeapTreeBranch : TreeBranch, IHashNode {
			private IHashNode nextHash;
			private IHashNode next;
			private IHashNode previous;

			private readonly ITransaction tran;

			internal HeapTreeBranch(ITransaction tran, long nodeId, int maxChildren)
				: base(nodeId, maxChildren) {
				this.tran = tran;
			}

			internal HeapTreeBranch(ITransaction tran, long nodeId, TreeBranch branch, int maxChildren)
				: base(nodeId, branch, maxChildren) {
				this.tran = tran;
			}

			public IHashNode NextHash {
				get { return nextHash; }
				set { nextHash = value; }
			}

			public ITransaction Transaction {
				get { return tran; }
			}

			public IHashNode Previous {
				get { return previous; }
				set { previous = value; }
			}

			public IHashNode Next {
				get { return next; }
				set { next = value; }
			}

			public override long MemoryAmount {
				get { return base.MemoryAmount + (8*4); }
			}
		}

		private class HeapTreeLeaf : TreeLeaf, IHashNode {

			private IHashNode next_hash;
			private IHashNode next_list;
			private IHashNode previous_list;

			private readonly ITransaction tran;

			private readonly byte[] data;

			private long nodeId;
			private int size;


			internal HeapTreeLeaf(ITransaction tran, long nodeId, int maxCapacity) {
				this.nodeId = nodeId;
				this.size = 0;
				this.tran = tran;
				this.data = new byte[maxCapacity];
			}

			internal HeapTreeLeaf(ITransaction tran, long nodeId, TreeLeaf toCopy, int maxCapacity)
				: base() {
				this.nodeId = nodeId;
				this.size = toCopy.Length;
				this.tran = tran;
				// Copy the data into an array in this leaf.
				data = new byte[maxCapacity];
				toCopy.Read(0, data, 0, size);
			}

			// ---------- Implemented from TreeLeaf ----------

			public override long Id {
				get { return nodeId; }
			}

			public override int Length {
				get { return size; }
			}

			public override int Capacity {
				get { return data.Length; }
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				Array.Copy(data, position, buffer, offset, count);
			}

			public override void Shift(int position, int off) {
				if (off != 0) {
					int new_size = Length + off;
					Array.Copy(data, position, data, position + off, Length - position);
					// Set the size
					size = new_size;
				}
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				Array.Copy(buffer, offset, data, position, count);
				if (position + count > size) {
					size = position + count;
				}
			}

			public override void SetLength(int value) {
				if (value< 0 || value > Capacity)
					throw new ArgumentException("Specified leaf size is out of range.");

				size = value;
			}

			public override long MemoryAmount {
				get { return 8 + 4 + data.Length + 64 + (8 * 4); }
			}

			// ---------- Implemented from HashNode ----------

			public IHashNode NextHash {
				get { return next_hash; }
				set { next_hash = value; }
			}

			public ITransaction Transaction {
				get { return tran; }
			}

			public IHashNode Previous {
				get { return previous_list; }
				set { previous_list = value; }
			}

			public IHashNode Next {
				get { return next_list; }
				set { next_list = value; }
			}
		}

	}
}