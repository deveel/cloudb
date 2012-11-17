//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class BlockServerTable : FixedSizeCollection {
		public BlockServerTable(IDataFile data) 
			: base(data, 16) {
		}

		public long[] this[long blockId] {
			get { return Get(blockId); }
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

			long blockId = Input.ReadInt64();
			long serverId = Input.ReadInt64();

			return new Record(blockId, serverId);
		}

		protected override int CompareRecordTo(long recordIndex, object recordKey) {
			SetPosition(recordIndex);

			long srcBlockId = Input.ReadInt64();
			long srcServerId = Input.ReadInt64();

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

			Output.Write(blockId);
			Output.Write(serverId);

			return true;
		}

		public long [] Get(long blockId) {
			long p = Search(new Record(blockId, 0));
			if (p < 0)
				p = -(p + 1);

			List<long> serverIdList = new List<long>();

			IDataFile dfile = DataFile;
			long size = dfile.Length;
			long pos = p * RecordSize;

			dfile.Position = pos;

			while (pos < size) {
				long readBlockId = Input.ReadInt64();
				long readServerId = Input.ReadInt64();

				if (readBlockId != blockId)
					break;

				serverIdList.Add(readServerId);
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

			IDataFile dfile = DataFile;
			long size = dfile.Length;
			long startPos = p * RecordSize;
			long pos = startPos;

			dfile.Position = pos;

			int count = 0;
			while (pos < size) {
				long readBlockId = Input.ReadInt64();
				long readServerId = Input.ReadInt64();

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
				Record destOb = (Record)ob;
				return (destOb.BlockId == BlockId &&
						destOb.ServerId == ServerId);
			}
		}
	}
}