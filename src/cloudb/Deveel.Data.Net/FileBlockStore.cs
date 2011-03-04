using System;
using System.IO;

using ComponentAce.Compression.Libs.zlib;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public sealed class FileBlockStore : IBlockStore {
		private readonly BlockId blockId;
		private readonly string fileName;
		private FileStream content;
		private int length;
		private StrongPagedAccess pagedAccess;
		
		public FileBlockStore(BlockId blockId, string fileName) {
			this.blockId = blockId;
			this.fileName = fileName;
		}
		
		private const int Header = 6 * 16384;
		
		public string FileName {
			get { return fileName; }
		}
		
		public bool Exists {
			get { return File.Exists(fileName); }
		}
		
		public int Type {
			get { return 1;	}
		}
		
		public bool Open() {
			// If the store file doesn't exist, create it
			if (!File.Exists(fileName)) {
				content = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 2048, FileOptions.WriteThrough);
				// Set the header table in the newly created file,
				content.SetLength(Header);
				content.Seek(0, SeekOrigin.Begin);
				length = Header;
				pagedAccess = new StrongPagedAccess(content, 2048);
				return true;
			} else {
				content = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 2048, FileOptions.WriteThrough);
				length = (int)content.Length;
				pagedAccess = new StrongPagedAccess(content, 2048);
				return false;
			}
		}
		
		public void Write(int dataId, byte[] buffer, int offset, int count) {
			// Arg checks
			if (count < 0 || count >= 65536) {
				throw new ArgumentException("len < 0 || len > 65535");
			}
			if (count + offset > buffer.Length) {
				throw new ArgumentException();
			}
			if (offset < 0) {
				throw new ArgumentException();
			}
			if (dataId < 0 || dataId >= 16384) {
				throw new ArgumentException("data_id out of range");
			}

			byte[] tmp_area = new byte[6];

			// Seek to the position of this data id in the table,
			int pos = dataId * 6;
			int dataIdPos = pagedAccess.ReadInt32(pos);
			int dataIdLength = ((int)pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;

			// These values should be 0, if not we've already written data here,
			if (dataIdPos != 0 || dataIdLength != 0)
				throw new ApplicationException("data_id previously written");
			
			// Write the content to the end of the file,
			content.Seek(length, SeekOrigin.Begin);
			content.Write(buffer, offset, count);
			pagedAccess.InvalidateSection(length, count);
			
			// Write the table entry,
			ByteBuffer.WriteInt4(length, tmp_area, 0);
			ByteBuffer.WriteInt2((short)count, tmp_area, 4);
			content.Seek(pos, SeekOrigin.Begin);
			content.Write(tmp_area, 0, 6);
			pagedAccess.InvalidateSection(pos, 6);
			
			// Set the new content length
			length = length + count;
		}

		public int Read(int dataId, byte[] buffer, int offset, int len) {
			if (dataId < 0 || dataId >= 16384)
				throw new ArgumentException("data_id out of range");

			// Seek to the position of this data id in the table,
			int pos = dataId*6;
			int dataIdPos = pagedAccess.ReadInt32(pos);
			int dataIdLength = ((int) pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;

			// If position for the data_id is 0, the data hasn't been written,
			len = Math.Min(len, dataIdLength);
			if (dataIdPos == 0)
				throw new BlockReadException("Data id " + dataId + " is empty (block " + blockId + ")");

			// Fetch the content,
			content.Seek(dataIdPos, SeekOrigin.Begin);
			return content.Read(buffer, offset, len);
		}

		public Stream OpenInputStream() {
			return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
		}

		public NodeSet GetNodeSet(int dataId) {
			if (dataId < 0 || dataId >= 16384)
				throw new ArgumentException("data_id out of range");

			// Seek to the position of this data id in the table,
			int pos = dataId*6;
			int dataIdPos = pagedAccess.ReadInt32(pos);
			int dataIdLength = ((int) pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;

			// If position for the data_id is 0, the data hasn't been written,
			byte[] buf = new byte[dataIdLength];
			if (dataIdPos == 0)
				throw new BlockReadException("Data id " + dataId + " is empty (block " + blockId + ")");

			// Fetch the content,
			content.Seek(dataIdPos, SeekOrigin.Begin);
			content.Read(buf, 0, dataIdLength);

			// Return as a nodeset object,
			return new SingleNodeSet(blockId, dataId, buf);
		}

		public void Delete(int dataId) {
			if (dataId < 0 || dataId >= 16384)
				throw new ArgumentException("data_id out of range");

			byte[] tmp_area = new byte[6];

			// Seek to the position of this data id in the table,
			int pos = dataId * 6;
			int dataIdPos = pagedAccess.ReadInt32(pos);
			int dataIdLength = ((int)pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;

			// Clear it,
			for (int i = 0; i < tmp_area.Length; ++i) {
				tmp_area[i] = 0;
			}
			// Write the cleared entry in,
			content.Seek(pos, SeekOrigin.Begin);
			content.Write(tmp_area, 0, 6);
			pagedAccess.InvalidateSection(pos, 6);
		}
		
		public void Flush() {
			if (content != null)
				content.Flush();
		}
		
		public void Close() {
			content.Close();
			content = null;
			length = 0;
			pagedAccess = null;
		}
		
		public long CreateChecksum() {
			Adler32 adler32 = new Adler32();
			long a1 = 0, a2 = 0;
			byte[] header_value = new byte[Header];
			content.Seek(0, SeekOrigin.Begin);
			content.Read(header_value, 0, Header);
			for (int i = 0; i < Header; i += 6) {
				int pos = ByteBuffer.ReadInt4(header_value, i);
				int len = ((int)ByteBuffer.ReadInt2(header_value, i + 4)) & 0x0FFFF;

				byte[] node = new byte[len];
				content.Seek(pos, SeekOrigin.Begin);
				content.Read(node, 0, len);

				if ((i & 0x01) == 0) {
					a1 = adler32.adler32(a1, node, 0, len);
				} else {
					a2 = adler32.adler32(a2, node, 0, len);
				}
			}

			// Return the 64 bit value checksum,
			return (a1 << 32) | a2;
		}
	}
}