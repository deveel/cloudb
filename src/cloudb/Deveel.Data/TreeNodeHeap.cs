//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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
			return node;
		}

		public void Delete(NodeId pointer) {
			int hashIndex = CalcHashValue(pointer);
			IHashNode hashNode = hash[hashIndex];
			IHashNode previous = null;
			while (hashNode != null &&
			       !(hashNode.Id.Equals(pointer))) {
				previous = hashNode;
				hashNode = hashNode.NextHash;
			}
			if (hashNode == null)
				throw new InvalidOperationException("Node not found!");

			if (previous == null) {
				hash[hashIndex] = hashNode.NextHash;
			} else {
				previous.NextHash = hashNode.NextHash;
			}

			// Remove from the double linked list structure,
			// If removed node at head.
			if (hashStart == hashNode) {
				hashStart = hashNode.Next;
				if (hashStart != null) {
					hashStart.Previous = null;
				} else {
					hashEnd = null;
				}
			}
				// If removed node at end.
			else if (hashEnd == hashNode) {
				hashEnd = hashNode.Previous;
				if (hashEnd != null) {
					hashEnd.Next = null;
				} else {
					hashStart = null;
				}
			} else {
				hashNode.Previous.Next = hashNode.Next;
				hashNode.Next.Previous = hashNode.Previous;
			}

			// Update the 'memory_used' variable
			memoryUsed -= hashNode.MemoryAmount;

			// KEEP STATS
			if (hashNode is TreeBranch) {
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

			private readonly byte[] data;

			private readonly NodeId nodeId;
			private int size;

			public HeapTreeLeaf(TreeSystemTransaction transaction, NodeId nodeId, int maxCapacity) {
				this.nodeId = nodeId;
				size = 0;
				this.transaction = transaction;
				data = new byte[maxCapacity];
			}

			public HeapTreeLeaf(TreeSystemTransaction transaction, NodeId nodeId, TreeLeaf toCopy, int capacity) {
				this.nodeId = nodeId;
				size = toCopy.Length;
				this.transaction = transaction;
				// Copy the data into an array in this leaf.
				data = new byte[capacity];
				toCopy.Read(0, data, 0, size);
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