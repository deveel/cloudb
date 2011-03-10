using System;
using System.IO;
using System.Text;

using Deveel.Data.Caching;

namespace Deveel.Data {
	public abstract class FixedSizeCollection {
		protected FixedSizeCollection(IDataFile data, int recordSize) {
			if (recordSize <= 0)
				throw new ArgumentException();

			this.data = data;
			input = new BinaryReader(new DataFileStream(data), Encoding.Unicode);
			output = new BinaryWriter(new DataFileStream(data), Encoding.Unicode);
			this.recordSize = recordSize;
			keyPositionCache = new MemoryCache(513, 750, 15);
		}

		private readonly IDataFile data;
		private readonly BinaryReader input;
		private readonly BinaryWriter output;
		private readonly int recordSize;
		private readonly Cache keyPositionCache;

		protected IDataFile DataFile {
			get { return data; }
		}

		protected BinaryReader Input {
			get { return input; }
		}

		protected BinaryWriter Output {
			get { return output; }
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
						if (cmp == 0)
							return i;
						if (cmp > 0)
							return -(i + 1);
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

		protected abstract object GetRecordKey(long recordIndex);

		protected abstract int CompareRecordTo(long recordIndex, object recordKey);

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