using System;
using System.Collections.Generic;

namespace Deveel.Data.Store {
	public interface ITreeStorageSystem {
		int MaxBranchSize { get; }

		int MaxLeafByteSize { get; }
		
		long NodeHeapMaxSize { get; }
		

		void CheckPoint();

		IList<T> FetchNodes<T>(long[] nids) where T : ITreeNode;
		
		bool LinkLeaf(Key key, long reference);

		void DisposeNode(long nid);

		Exception SetErrorState(Exception error);

		void CheckErrorState();

		IList<long> Persist(TreeWrite write);
	}
}