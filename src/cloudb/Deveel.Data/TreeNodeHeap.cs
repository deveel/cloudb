using System;
using System.Collections.Generic;

using Deveel.Data.Store;

namespace Deveel.Data {
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

		private int CalcHashValue(NodeId p) {
			int hc = p.GetHashCode();
			if (hc < 0) {
				hc = -hc;
			}
			return hc % hash.Length;
		}

		private void PutInHash(IHashNode node) {
			NodeId nodeId = node.Id;

			int hashIndex = CalcHashValue(nodeId);
			IHashNode oldNode = hash[hashIndex];
			hash[hashIndex] = node;
			node.NextHash = oldNode;

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

			// STATS
			if (node is TreeBranch) {
				++totalBranchNodeCount;
			} else {
				++totalLeafNodeCount;
			}
		}


		private NodeId NextNodePointer() {
			long p = nodeIdSeq;
			++nodeIdSeq;
			// ISSUE: What happens if the node id sequence overflows?
			//   The boundary is large enough that if we were to create a billion
			//   nodes a second continuously, it would take 18 years to overflow.
			nodeIdSeq = nodeIdSeq & 0x0FFFFFFFFFFFFFFFL;

			return NodeId.CreateInMemoryNode(p);
		}

		public ITreeNode FetchNode(NodeId pointer) {
			// Fetches the node out of the heap hash array.
			int hashIndex = CalcHashValue(pointer);
			IHashNode hashNode = hash[hashIndex];
			while (hashNode != null &&
				   !hashNode.Id.Equals(pointer)) {
				hashNode = hashNode.NextHash;
			}

			return hashNode;
		}

		public TreeBranch CreateBranch(TreeSystemTransaction tran, int maxBranchChildren) {
			NodeId p = NextNodePointer();
			HeapTreeBranch node = new HeapTreeBranch(tran, p, maxBranchChildren);
			PutInHash(node);
			return node;
		}

		public TreeLeaf CreateLeaf(TreeSystemTransaction tran, Key key, int maxLeafSize) {
			NodeId p = NextNodePointer();
			HeapTreeLeaf node = new HeapTreeLeaf(tran, p, maxLeafSize);
			PutInHash(node);
			return node;
		}

		public ITreeNode Copy(ITreeNode nodeToCopy, int maxBranchSize, int maxLeafSize, TreeSystemTransaction tran) {
			// Create a new pointer for the copy
			NodeId p = NextNodePointer();
			IHashNode node;
			if (nodeToCopy is TreeLeaf) {
				node = new HeapTreeLeaf(tran, p, (TreeLeaf) nodeToCopy, maxLeafSize);
			} else {
				node = new HeapTreeBranch(tran, p, (TreeBranch) nodeToCopy, maxBranchSize);
			}
			PutInHash(node);
			// Return pointer to node
			return (ITreeNode) node;
		}

		public void Delete(NodeId pointer) {
			int hash_index = CalcHashValue(pointer);
			IHashNode hash_node = hash[hash_index];
			IHashNode previous = null;
			while (hash_node != null &&
			       !(hash_node.Id.Equals(pointer))) {
				previous = hash_node;
				hash_node = hash_node.NextHash;
			}
			if (hash_node == null)
				throw new InvalidOperationException("Node not found!");

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

			// KEEP STATS
			if (hash_node is TreeBranch) {
				--totalBranchNodeCount;
			} else {
				--totalLeafNodeCount;
			}
		}

		internal void FlushCache() {
			IList<IHashNode> toFlush = null;
			// If the memory use is above some limit then we need to flush out some
			// of the nodes,
			if (memoryUsed > maxMemoryLimit) {
				int allNodeCount = totalBranchNodeCount + totalLeafNodeCount;
				// The number to clean,
				int toClean = (int) (allNodeCount*0.30);

				// Make an array of all nodes to flush,
				toFlush = new List<IHashNode>(toClean);
				// Pull them from the back of the list,
				IHashNode node = hashEnd;
				while (toClean > 0 && node != null) {
					toFlush.Add(node);
					node = node.Previous;
					--toClean;
				}
			}

			// If there are nodes to flush,
			if (toFlush != null) {
				// Read each group and call the node flush routine,

				// The mapping of transaction to node list
				Dictionary<TreeSystemTransaction, List<NodeId>> tranMap = new Dictionary<TreeSystemTransaction, List<NodeId>>();
				// Read the list backwards,
				for (int i = toFlush.Count - 1; i >= 0; --i) {
					IHashNode node = toFlush[i];
					// Get the transaction of this node,
					TreeSystemTransaction tran = node.Transaction;
					// Find the list of this transaction,
					List<NodeId> nodeList = tranMap[tran];
					if (nodeList == null) {
						nodeList = new List<NodeId>(toFlush.Count);
						tranMap.Add(tran, nodeList);
					}
					// Add to the list
					nodeList.Add(node.Id);
				}

				// Now read the key and dispatch the clean up to the transaction objects,
				foreach (KeyValuePair<TreeSystemTransaction, List<NodeId>> pair in tranMap) {
					TreeSystemTransaction tran = pair.Key;
					List<NodeId> nodeList = pair.Value;
					// Convert to a 'NodeId[]' array,
					NodeId[] refs = nodeList.ToArray();

					// Sort the references,
					Array.Sort(refs);

					// Tell the transaction to clean up these nodes,
					tran.FlushNodesToStore(refs);
				}

			}
		}

		#region IHashNode

		private interface IHashNode : ITreeNode {
			IHashNode NextHash { get; set; }

			IHashNode Previous { get; set; }

			IHashNode Next { get; set; }

			TreeSystemTransaction Transaction { get; }
		}

		#endregion

		#region HeapTreeBranch

		private class HeapTreeBranch : TreeBranch, IHashNode {
			private readonly TreeSystemTransaction transaction;
			private IHashNode nextHash;
			private IHashNode next;
			private IHashNode previous;

			public HeapTreeBranch(TreeSystemTransaction transaction, NodeId nodeId, int maxChildrenCount)
				: base(nodeId, maxChildrenCount) {
				this.transaction = transaction;
			}

			public HeapTreeBranch(TreeSystemTransaction transaction, NodeId nodeId, TreeBranch branch, int maxChildrenCount)
				: base(nodeId, branch, maxChildrenCount) {
				this.transaction = transaction;
			}

			public IHashNode NextHash {
				get { return nextHash; }
				set { nextHash = value; }
			}

			public IHashNode Previous {
				get { return previous; }
				set { previous = value; }
			}

			public IHashNode Next {
				get { return next; }
				set { next = value; }
			}

			public TreeSystemTransaction Transaction {
				get { return transaction; }
			}

			public override long MemoryAmount {
				get { return base.MemoryAmount + (8*4); }
			}
		}

		#endregion

		#region HeapTreeLeaf

		private class HeapTreeLeaf : TreeLeaf, IHashNode {
			private readonly TreeSystemTransaction transaction;
			private IHashNode nextHash;
			private IHashNode next;
			private IHashNode previous;

			private byte[] data;

			private NodeId nodeId;
			private int size;

			public HeapTreeLeaf(TreeSystemTransaction transaction, NodeId nodeId, int maxCapacity) {
				this.nodeId = nodeId;
				size = 0;
				this.transaction = transaction;
				data = new byte[maxCapacity];
			}

			public HeapTreeLeaf(TreeSystemTransaction transaction, NodeId nodeId, TreeLeaf to_copy, int capacity) {
				this.nodeId = nodeId;
				this.size = to_copy.Length;
				this.transaction = transaction;
				// Copy the data into an array in this leaf.
				this.data = new byte[capacity];
				to_copy.Read(0, data, 0, size);
			}

			public override int Length {
				get { return size; }
			}

			public override int Capacity {
				get { return data.Length; }
			}

			public override NodeId Id {
				get { return nodeId; }
			}

			public override long MemoryAmount {
				get {
					// The size of the member variables + byte estimate for heap use for
					// object maintenance.
					return 8 + 4 + data.Length + 64 + (8*4);
				}
			}

			public override void SetLength(int value) {
				if (value < 0 || value > Capacity)
					throw new ArgumentException("Leaf size error: " + value);

				size = value;
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				Array.Copy(data, position, buffer, offset, count);
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				Array.Copy(buffer, offset, data, position, count);
				if (position + count > size) {
					size = position + count;
				}
			}

			public override void WriteTo(IAreaWriter area) {
				area.Write(data, 0, Length);
			}

			public override void Shift(int position, int offset) {
				if (offset != 0) {
					int newSize = Length + offset;
					Array.Copy(data, position,
					           data, position + offset, Length - position);
					// Set the size
					size = newSize;
				}
			}

			public IHashNode NextHash {
				get { return nextHash; }
				set { nextHash = value; }
			}

			public IHashNode Previous {
				get { return previous; }
				set { previous = value; }
			}

			public IHashNode Next {
				get { return next; }
				set { next = value; }
			}

			public TreeSystemTransaction Transaction {
				get { return transaction; }
			}
		}

		#endregion
	}
}