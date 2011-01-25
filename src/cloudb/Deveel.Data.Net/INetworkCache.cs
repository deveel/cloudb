using System;
using System.Collections.Generic;

using Deveel.Data.Store;

namespace Deveel.Data.Net {
	public interface INetworkCache : ITreeNodeCache {
		void SetServers(long blockId, IList<BlockServerElement> servers, int ttlHint);

		IList<BlockServerElement> GetServers(long blockId);

		void RemoveServers(long block_id);
	}
}