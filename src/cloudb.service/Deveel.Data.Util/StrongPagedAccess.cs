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
using System.IO;

namespace Deveel.Data.Util {
	internal sealed class StrongPagedAccess {
		private readonly Stream input;
		private readonly Dictionary<long, byte[]> cache;
		private readonly int pageSize;
		private int cacheHit;
		private int cacheMiss;

		public StrongPagedAccess(Stream input, int pageSize) {
			this.input = input;
			this.pageSize = pageSize;
			cache = new Dictionary<long, byte[]>();
		}

		private byte[] FetchPage(long pageNo) {
			byte[] page;
			if (!cache.TryGetValue(pageNo, out page)) {
				page = new byte[pageSize];
				input.Seek(pageNo * pageSize, SeekOrigin.Begin);
				int n = 0;
				int sz = pageSize;
				while (sz > 0) {
					int readCount = input.Read(page, n, sz);
					if (readCount == 0) {
						// eof
						break;
					}
					n += readCount;
					sz -= readCount;
				}
				cache.Add(pageNo, page);
				++cacheMiss;
			} else {
				++cacheHit;
			}
			return page;
		}

		public int CacheHits {
			get { return cacheHit; }
		}

		public int CacheMiss {
			get { return cacheMiss; }
		}

		public void ClearCache(int number) {
			if (cache.Count > number) {
				cache.Clear();
			}
		}

		public void InvalidateSection(long pos, int sz) {
			while (sz > 0) {
				// Get the page,
				long pageNo = (pos / pageSize);
				// Remove it from the cache,
				cache.Remove(pageNo);

				long nextPagePos = (pageNo + 1) * pageSize;
				int skip = (int)(nextPagePos - pos);

				// Go to the next page,
				sz -= skip;
				pos += skip;
			}
		}

		public int Read(long pos, byte[] buffer, int offset, int length) {
			// Get the page,
			long pageNo = (pos / pageSize);
			// Is the page in the cache?
			byte[] page = FetchPage(pageNo);

			// The offset of the position inside the page,
			int pageOffset = (int)(pos - (pageNo * pageSize));
			// The maximum we can read,
			int maxRead = pageSize - pageOffset;
			// How much we are going to read,
			int toRead = Math.Min(length, maxRead);

			// Go ahead and copy the content,
			Array.Copy(page, pageOffset, buffer, offset, toRead);

			return toRead;
		}

		public byte ReadByte(long pos) {
			byte[] buffer = new byte[1];
			Read(pos, buffer, 0, 1);
			return buffer[0];
		}

		public long ReadInt64(long pos) {
			byte[] buffer = new byte[8];
			Read(pos, buffer, 0, 8);
			return ByteBuffer.ReadInt8(buffer, 0);
		}

		public int ReadInt32(long pos) {
			byte[] buffer = new byte[4];
			Read(pos, buffer, 0, 4);
			return ByteBuffer.ReadInt4(buffer, 0);
		}

		public short ReadInt16(long pos) {
			byte[] buffer = new byte[2];
			Read(pos, buffer, 0, 2);
			return ByteBuffer.ReadInt2(buffer, 0);
		}

	}
}