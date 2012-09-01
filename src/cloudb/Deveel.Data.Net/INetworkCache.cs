using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public interface INetworkCache {
		void SetNode(DataAddress address, ITreeNode node);

		ITreeNode GetNode(DataAddress address);

		void RemoveNode(DataAddress address);


		PathInfo GetPathInfo(String pathName);

		void SetPathInfo(String pathName, PathInfo pathInfo);


		IList<BlockServerElement> GetServersWithBlock(BlockId blockId);

		void SetServersForBlock(BlockId blockId, IList<BlockServerElement> servers, int ttlHint);

		void RemoveServersWithBlock(BlockId blockId);

	}
}