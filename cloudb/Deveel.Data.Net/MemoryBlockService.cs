using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class MemoryBlockService : BlockService {
		private readonly Dictionary<BlockId, MemoryBlockStore> blocks = new Dictionary<BlockId, MemoryBlockStore>();

		public MemoryBlockService(IServiceConnector connector) 
			: base(connector) {
		}

		protected override BlockContainer LoadBlock(BlockId blockId) {
			MemoryBlockStore store;
			if (!blocks.TryGetValue(blockId, out store)) {
				store = new MemoryBlockStore(blockId);
				blocks[blockId] = store;
			}

			return new BlockContainer(blockId, store);
		}

		protected override BlockId[] ListBlocks() {
			BlockId[] ids = new BlockId[blocks.Count];
			blocks.Keys.CopyTo(ids, 0);
			return ids;
		}
	}
}