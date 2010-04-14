using System;

using Deveel.Data.Net;

namespace Deveel.Data.Store {
	public interface ITreeNodeCache {
		void SetNode(DataAddress address, ITreeNode node);

		ITreeNode GetNode(DataAddress address);

		void RemoveNode(DataAddress address);
	}
}