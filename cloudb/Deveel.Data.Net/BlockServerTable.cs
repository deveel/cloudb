using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class BlockServerTable : FixedSizeCollection {
		public BlockServerTable(DataFile data) 
			: base(data, 24) {
		}

		public long[] this[BlockId block_id] {
			get { return Get(block_id); }
		}

		public BlockId LastBlockId {
			get {
				long p = Count - 1;
				Record item = (Record)GetRecordKey(p);
				return item.BlockId;
			}
		}

		#region Overrides of FixedSizeCollection

		protected override object GetRecordKey(long recordIndex) {
			SetPosition(recordIndex);

			long blockIdH = DataFile.ReadInt64();
			long blockIdL = DataFile.ReadInt64();
			BlockId blockId = new BlockId(blockIdH, blockIdL);
			long serverId = DataFile.ReadInt64();

			return new Record(blockId, serverId);
		}

		protected override int CompareRecordTo(long recordIndex, object recordKey) {
			SetPosition(recordIndex);

			long srcBlockIdH = DataFile.ReadInt64();
			long srcBlockIdL = DataFile.ReadInt64();
			BlockId srcBlockId = new BlockId(srcBlockIdH, srcBlockIdL);
			long srcServerId = DataFile.ReadInt64();

			Record dstRecord = (Record) recordKey;

			BlockId dstBlockId = dstRecord.BlockId;

			int cmp = srcBlockId.CompareTo(dstBlockId);
			if (cmp > 0)
				return 1;
			if (cmp < 0)
				return -1;

			// If identical block items, sort by the server identifier
			long dstServerId = dstRecord.ServerId;
			if (srcServerId > dstServerId)
				return 1;
			if (srcServerId < dstServerId)
				return -1;

			// Equal,
			return 0;
		}

		#endregion

		public bool Add(BlockId blockId, long serverId) {
			if (serverId < 0)
				throw new ArgumentOutOfRangeException("serverId");

			long p = Search(new Record(blockId, serverId));

			if (p >= 0)
				return false;

			p = -(p + 1);

			InsertEmptyRecord(p);

			SetPosition(p);

			DataFile.Write(blockId.High);
			DataFile.Write(blockId.Low);
			DataFile.Write(serverId);

			return true;
		}

		public bool Add(BlockId blockId, long[] serverIds) {
			bool b = true;
			foreach (long serverId in serverIds) {
				b &= Add(blockId, serverId);
			}
			return b;
		}

		public long [] Get(BlockId blockId) {
			long p = Search(new Record(blockId, 0));
			if (p < 0)
				p = -(p + 1);

			List<long> serverIdList = new List<long>();

			DataFile dfile = DataFile;
			long size = dfile.Length;
			long pos = p * RecordSize;

			dfile.Position = pos;

			while (pos < size) {
				long readBlockIdH = dfile.ReadInt64();
				long readBlockIdL = dfile.ReadInt64();
				BlockId readBlockId = new BlockId(readBlockIdH, readBlockIdL);
				long readServerId = dfile.ReadInt64();

				if (!readBlockId.Equals(blockId))
					break;

				serverIdList.Add(readServerId);
				pos += RecordSize;
			}

			return serverIdList.ToArray();
		}

		public string[] GetRange(long p1, long p2) {
			if ((p2 - p1) > Int32.MaxValue)
				throw new OverflowException();

			int sz = (int)(p2 - p1);
			string[] arr = new string[sz];
			for (int p = 0; p < sz; ++p) {
				Record item = (Record)GetRecordKey(p1 + p);
				arr[p] = item.BlockId + "=" + item.ServerId;
			}
			return arr;
		}

		public int Remove(BlockId blockId) {
			long p = Search(new Record(blockId, 0));
			if (p < 0)
				p = -(p + 1);

			DataFile dfile = DataFile;
			long size = dfile.Length;
			long startPos = p * RecordSize;
			long pos = startPos;

			dfile.Position = pos;

			int count = 0;
			while (pos < size) {
				long readBlockIdH = dfile.ReadInt64();
				long readBlockIdL = dfile.ReadInt64();
				BlockId readBlockId = new BlockId(readBlockIdH, readBlockIdL);
				long readServerId = dfile.ReadInt64();

				if (!readBlockId.Equals(blockId))
					break;

				pos += RecordSize;
				++count;
			}

			if ((startPos - pos) != 0) {
				dfile.Position = pos;
				dfile.Shift(startPos - pos);
			}

			return count;
		}

		public bool Remove(BlockId blockId, long serverId) {
			long p = Search(new Record(blockId, serverId));
			if (p < 0)
				return false;

			RemoveAt(p);
			return true;
		}

		public object[] GetKeyValueChunk(BlockId min, int rangeSize) {
			List<BlockId> keys = new List<BlockId>();
			List<long> values = new List<long>();

			// Search for the first record item
			Record item = new Record(min, 0);
			long p = Search(item);
			if (p < 0) {
				// If the record wasn't found, we set p to the insert location
				p = -(p + 1);
			}
			// Fetch the records,
			DataFile dfile = DataFile;
			long size = dfile.Length;
			long startLoc = p * RecordSize;
			long loc = startLoc;
			int count = 0;
			dfile.Position = loc;

			BlockId lastBlockId = null;

			while (count < rangeSize && loc < size) {
				long readBlockIdH = dfile.ReadInt64();
				long readBlockIdL = dfile.ReadInt64();
				BlockId readBlockId = new BlockId(readBlockIdH, readBlockIdL);
				long readServerId = dfile.ReadInt64();

				// Count each time we go to a new block,
				if (lastBlockId == null || !lastBlockId.Equals(readBlockId)) {
					lastBlockId = readBlockId;
					++count;
				}

				keys.Add(readBlockId);
				values.Add(readServerId);

				// Add this record to the area being deleted,
				loc += RecordSize;
			}

			return new object[] {keys, values};
		}

		private sealed class Record {
			internal Record(BlockId blockId, long serverId) {
				BlockId = blockId;
				ServerId = serverId;
			}

			public readonly BlockId BlockId;
			public readonly long ServerId;

			public override int GetHashCode() {
				return (int)(BlockId.GetHashCode() + ServerId);
			}

			public override bool Equals(Object ob) {
				Record dest_ob = (Record)ob;
				return (dest_ob.BlockId.Equals(BlockId) &&
						dest_ob.ServerId == ServerId);
			}
		}
	}
}