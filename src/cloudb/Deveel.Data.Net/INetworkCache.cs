using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public interface INetworkCache : ITreeNodeCache {
		void SetServers(BlockId blockId, IList<BlockServerElement> servers, int ttlHint);

		IList<BlockServerElement> GetServers(BlockId blockId);

		void RemoveServers(BlockId blockId);

		PathInfo GetPathInfo(string pathName);

		void SetPathInfo(string pathName, PathInfo pathInfo);
	}
}