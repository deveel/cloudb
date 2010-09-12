using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class MemoryBlockService : BlockService {
		private readonly Dictionary<long, MemoryBlockStore> blocks = new Dictionary<long, MemoryBlockStore>();

		public MemoryBlockService(IServiceConnector connector) 
			: base(connector) {
		}

		protected override BlockContainer LoadBlock(long blockId) {
			MemoryBlockStore store;
			if (!blocks.TryGetValue(blockId, out store)) {
				store = new MemoryBlockStore(blockId);
				blocks[blockId] = store;
			}

			return new BlockContainer(blockId, store);
		}

		protected override long[] ListBlocks() {
			long[] ids = new long[blocks.Count];
			blocks.Keys.CopyTo(ids, 0);
			return ids;
		}
	}
}