using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Deveel.Data.Store {
	public sealed class TreeWrite {
		private readonly List<ITreeNode> leafNodes = new List<ITreeNode>();
		private readonly List<ITreeNode> branchNodes = new List<ITreeNode>();
		private readonly Dictionary<long, int> links = new Dictionary<long, int>();

		internal const int BranchPoint = 65536 * 16384;

		public ReadOnlyCollection<ITreeNode> LeafNodes {
			get { return leafNodes.AsReadOnly(); }
		}

		public ReadOnlyCollection<ITreeNode> BranchNodes {
			get { return branchNodes.AsReadOnly(); }
		}

		internal int LookupRef(int branchId, int childIndex) {
			// NOTE: Returns the reference for branches normalized on a node list that
			//  includes branch and leaf nodes together in order branch + leaf.

			branchId = (branchId + BranchPoint);

			// Turn {branch_id, child_i} into a key,
			long key = ((long)branchId << 16) + childIndex;
			int ref_id = links[key];
			return ref_id >= BranchPoint ? ref_id - BranchPoint : ref_id + branchNodes.Count;
		}

		public void BranchLink(int branchId, int childIndex, int childId) {
			// Turn {branchId, childIndex} into a key,
			long key = ((long)branchId << 16) + childIndex;
			links[key] = childId;
		}

		public int NodeWrite(ITreeNode node) {
			if (node is TreeBranch) {
				branchNodes.Add(node);
				return (branchNodes.Count - 1) + BranchPoint;
			} else {
				leafNodes.Add(node);
				return leafNodes.Count - 1;
			}
		}
	}
}