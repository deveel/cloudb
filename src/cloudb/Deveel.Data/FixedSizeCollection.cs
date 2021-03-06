﻿//
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
using System.IO;

using Deveel.Data.Caching;

namespace Deveel.Data {
	public abstract class FixedSizeCollection {
		protected FixedSizeCollection(IDataFile data, int recordSize) {
			if (recordSize <= 0)
				throw new ArgumentException();

			this.data = data;
			this.recordSize = recordSize;
			keyPositionCache = new MemoryCache(513, 750, 15);

			fileReader = new BinaryReader(new DataFileStream(data));
			fileWriter = new BinaryWriter(new DataFileStream(data));
		}

		private readonly IDataFile data;
		private readonly BinaryReader fileReader;
		private readonly BinaryWriter fileWriter;
		private readonly int recordSize;
		private readonly Cache keyPositionCache;

		protected IDataFile DataFile {
			get { return data; }
		}

		protected BinaryReader Input {
			get { return fileReader; }
		}

		protected BinaryWriter Output {
			get { return fileWriter; }
		}

		public int RecordSize {
			get { return recordSize; }
		}

		public long Count {
			get { return DataFile.Length/RecordSize; }
		}

		private long GetKeyPosition(object key) {
			long rec_start = 0;
			long rec_end = Count;

			long low = rec_start;
			long high = rec_end - 1;

			while (low <= high) {

				if (high - low <= 2) {
					for (long i = low; i <= high; ++i) {
						int cmp = CompareRecordTo(i, key);
						if (cmp == 0) {
							return i;
						} else if (cmp > 0) {
							return -(i + 1);
						}
					}
					return -(high + 2);
				}

				long mid = (low + high) / 2;
				int cmp1 = CompareRecordTo(mid, key);

				if (cmp1 < 0) {
					low = mid + 1;
				} else if (cmp1 > 0) {
					high = mid - 1;
				} else {
					high = mid;
				}
			}
			return -(low + 1);  // key not found.
		}

		protected abstract object GetRecordKey(long recordPos);

		protected abstract int CompareRecordTo(long recordPos, object recordKey);

		protected void SetPosition(long record_num) {
			data.Position = record_num * RecordSize;
		}

		protected void InsertEmptyRecord(long record_num) {
			// If we are inserting the record at the end, we must grow the size of the
			// file by the size of the record.
			if (record_num == Count) {
				DataFile.SetLength((record_num + 1) * RecordSize);
			}
				// Otherwise we shift the data in the file so that we have space to insert
				// the record.
			else {
				SetPosition(record_num);
				DataFile.Shift(RecordSize);
			}
		}

		protected void RemoveAt(long record_num) {
			// Position on the record
			SetPosition(record_num + 1);
			// Shift the data in the file
			DataFile.Shift(-RecordSize);
		}

		public long Search(object key) {
			// Check the cache
			object v = keyPositionCache.Get(key);
			long pos;
			if (v == null) {
				pos = GetKeyPosition(key);
				if (pos >= 0) {
					keyPositionCache.Set(key, pos);
				}
			} else {
				pos = (long)v;
			}
			return pos;
		}

		public bool Remove(object key) {
			// Search for the position of the record
			long pos = Search(key);
			if (pos >= 0) {
				// Record found, so remove it
				RemoveAt(pos);
				// And remove from the cache
				keyPositionCache.Remove(key);
				return true;
			}
			return false;
		}

		public bool Contains(object key) {
			return Search(key) >= 0;
		}
	}
}