using System;
using System.Collections.Generic;

using Deveel.Data.Store;

namespace Deveel.Data.Net {
	public sealed class BlockServerTable : FixedSizeCollection {
		public BlockServerTable(DataFile data) 
			: base(data, 16) {
		}

		public long[] this[long block_id] {
			get { return Get(block_id); }
		}

		public long LastBlockId {
			get {
				long p = Count - 1;
				Record item = (Record)GetRecordKey(p);
				return item.BlockId;
			}
		}

		#region Overrides of FixedSizeCollection

		protected override object GetRecordKey(long recordIndex) {
			SetPosition(recordIndex);

			long blockId = DataFile.ReadInt64();
			long serverId = DataFile.ReadInt64();

			return new Record(blockId, serverId);
		}

		protected override int CompareRecordTo(long recordIndex, object recordKey) {
			SetPosition(recordIndex);

			long srcBlockId = DataFile.ReadInt64();
			long srcServerId = DataFile.ReadInt64();

			Record dstRecord = (Record) recordKey;

			long dstBlockId = dstRecord.BlockId;
			if (srcBlockId > dstBlockId)
				return 1;
			if (srcBlockId < dstBlockId)
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

		public bool Add(long blockId, long serverId) {
			long p = Search(new Record(blockId, serverId));

			if (p >= 0)
				return false;

			p = -(p + 1);

			InsertEmptyRecord(p);

			SetPosition(p);

			DataFile.Write(blockId);
			DataFile.Write(serverId);

			return true;
		}

		public long [] Get(long blockId) {
			long p = Search(new Record(blockId, 0));
			if (p < 0)
				p = -(p + 1);

			List<long> serverIdList = new List<long>();

			DataFile dfile = DataFile;
			long size = dfile.Length;
			long pos = p * RecordSize;

			dfile.Position = pos;

			while (pos < size) {
				long read_block_id = dfile.ReadInt64();
				long read_server_id = dfile.ReadInt64();

				if (read_block_id != blockId)
					break;

				serverIdList.Add(read_server_id);
				pos += RecordSize;
			}

			return serverIdList.ToArray();
		}

		public long[] GetRange(long p1, long p2) {
			if ((p2 - p1) > Int32.MaxValue)
				throw new OverflowException();

			int sz = (int)(p2 - p1);
			long[] arr = new long[sz * 2];
			for (int p = 0; p < sz; ++p) {
				Record item = (Record)GetRecordKey(p1 + p);
				arr[(p * 2) + 0] = item.BlockId;
				arr[(p * 2) + 1] = item.ServerId;
			}
			return arr;
		}

		public int Remove(long blockId) {
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
				long readBlockId = dfile.ReadInt64();
				long readServerId = dfile.ReadInt64();

				if (readBlockId != blockId)
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

		public bool Remove(long blockId, long serverId) {
			long p = Search(new Record(blockId, serverId));
			if (p < 0)
				return false;

			RemoveAt(p);
			return true;
		}

		private sealed class Record {
			internal Record(long blockId, long serverId) {
				BlockId = blockId;
				ServerId = serverId;
			}

			public readonly long BlockId;
			public readonly long ServerId;

			public override int GetHashCode() {
				return (int)(BlockId + ServerId);
			}

			public override bool Equals(Object ob) {
				Record dest_ob = (Record)ob;
				return (dest_ob.BlockId == BlockId &&
						dest_ob.ServerId == ServerId);
			}
		}
	}
}