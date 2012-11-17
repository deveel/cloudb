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
using System.Collections.ObjectModel;

namespace Deveel.Data {
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

		public int LookupRef(int branchId, int childIndex) {
			// NOTE: Returns the reference for branches normalized on a node list that
			//  includes branch and leaf nodes together in order branch + leaf.

			branchId = (branchId + BranchPoint);

			// Turn {branch_id, child_i} into a key,
			long key = ((long)branchId << 16) + childIndex;
			int refId = links[key];
			return refId >= BranchPoint ? refId - BranchPoint : refId + branchNodes.Count;
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
			}

			leafNodes.Add(node);
			return leafNodes.Count - 1;
		}
	}
}