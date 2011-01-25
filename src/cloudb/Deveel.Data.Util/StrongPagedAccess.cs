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
			this.cache = new Dictionary<long, byte[]>();
		}

		private byte[] FetchPage(long page_no) {
			byte[] page;
			if (!cache.TryGetValue(page_no, out page)) {
				page = new byte[pageSize];
				input.Seek(page_no * pageSize, SeekOrigin.Begin);
				int n = 0;
				int sz = pageSize;
				while (sz > 0) {
					int read_count = input.Read(page, n, sz);
					if (read_count == 0) {
						// eof
						break;
					}
					n += read_count;
					sz -= read_count;
				}
				cache.Add(page_no, page);
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