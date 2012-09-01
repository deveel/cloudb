using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class MemoryBlockService : BlockService {
		private readonly Dictionary<BlockId, Stream> blockData;
 
		public MemoryBlockService(IServiceConnector connector) 
			: base(connector) {
			blockData = new Dictionary<BlockId, Stream>();
		}

		protected override BlockData GetBlockData(BlockId blockId, int blockType) {
			return new MemoryBlockData(this, blockId, blockType);
		}

		protected override IBlockStore GetBlockStore(BlockId blockId) {
			return new MemoryBlockStore(blockId);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				foreach (KeyValuePair<BlockId, Stream> pair in blockData) {
					pair.Value.Dispose();
				}
			}

			base.Dispose(disposing);
		}

		#region MemoryBlockData

		class MemoryBlockData : BlockData {
			private readonly MemoryBlockService blockService;

			public MemoryBlockData(MemoryBlockService blockService, BlockId blockId, int blockType) 
				: base(blockId, blockType) {
				this.blockService = blockService;
			}

			public override bool Exists {
				get { return blockService.blockData.ContainsKey(BlockId); }
			}

			public override Stream OpenRead() {
				Stream stream;

				lock (blockService.blockData) {
					if (!blockService.blockData.TryGetValue(BlockId, out stream)) {
						stream = new MemoryStream(2048);
						blockService.blockData[BlockId] = stream;
					}
				}

				return stream;
			}

			public override Stream OpenWrite() {
				Stream stream;

				lock (blockService.blockData) {
					if (!blockService.blockData.TryGetValue(BlockId, out stream)) {
						stream = new MemoryStream(2048);
						blockService.blockData[BlockId] = stream;
					}
				}

				return stream;
			}
		}

		#endregion
	}
}