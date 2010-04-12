using System;
using System.Collections.Generic;

namespace Deveel.Data.Store {
	public interface ITreeStorageSystem {
		T GetConfigValue<T>(string key);

		void CheckPoint();

		IList<T> FetchNodes<T>(long[] nids) where T : ITreeNode;

		void DisposeNode(long nid);

		Exception SetErrorState(Exception error);

		void CheckErrorState();

		IList<long> Persist(TreeWrite write);
	}
}