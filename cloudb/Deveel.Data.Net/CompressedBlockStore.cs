using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public sealed class CompressedBlockStore : IBlockStore {
		private readonly BlockId blockId;
		private readonly string fileName;
		private FileStream content;
		private StrongPagedAccess pagedAccess;

		public CompressedBlockStore(BlockId blockId, string fileName) {
			this.blockId = blockId;
			this.fileName = fileName;
		}

		public bool Exists {
			get { return File.Exists(fileName); }
		}

		public int Type {
			get { return 2; }
		}

		public bool Open() {
			// If the store file doesn't exist, throw an error. We can't create
			// compressed files, they are made by calling the 'compress'.
			if (!File.Exists(fileName))
				throw new ApplicationException("Compressed file '" + fileName + "' was not found.");

			content = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.None, 2048, FileOptions.WriteThrough);
			pagedAccess = new StrongPagedAccess(content, 2048);
			return false;
		}

		public void Write(int dataId, byte[] buffer, int offset, int length) {
			throw new NotSupportedException();
		}

		public int Read(int dataId, byte[] buffer, int offset, int length) {
			if (dataId < 0 || dataId >= 16384)
				throw new ArgumentException("dataId out of range");

			try {
				int dataPointer = dataId;
				int pos = dataPointer * 6;
				int dataIdPos = pagedAccess.ReadInt32(pos);
				int dataIdLength = ((int)pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;

				if (dataIdPos < 0) {
					dataPointer = -(dataIdPos + 1);
					pos = dataPointer * 6;
					dataIdPos = pagedAccess.ReadInt32(pos);
					dataIdLength = ((int)pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;
				}

				length = Math.Min(length, dataIdLength);

				// Read the encoded form into a byte[] array,
				content.Seek(dataIdPos, SeekOrigin.Begin);
				return content.Read(buffer, offset, length);
			} catch (IOException e) {
				// We wrap this IOException around a BlockReadException. This can only
				// indicate a corrupt compressed block file or access to a dataId that
				// is out of range of the nodes stored in this file.
				throw new BlockReadException("IOError reading data from block file", e);
			}
		}

		public Stream OpenInputStream() {
			return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
		}

		public NodeSet GetNodeSet(int dataId) {
			if (dataId < 0 || dataId >= 16384)
				throw new ArgumentException("dataId out of range");

			try {
				int dataPointer = dataId;
				int pos = dataPointer * 6;
				int dataIdPos = pagedAccess.ReadInt32(pos);
				int dataIdLength = ((int)pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;

				if (dataIdPos < 0) {
					dataPointer = -(dataIdPos + 1);
					pos = dataPointer * 6;
					dataIdPos = pagedAccess.ReadInt32(pos);
					dataIdLength = ((int)pagedAccess.ReadInt16(pos + 4)) & 0x0FFFF;
				}

				// Fetch the node set,
				List<int> nodeIds = new List<int>(24);
				nodeIds.Add(dataPointer);
				while (true) {
					++dataPointer;
					pos += 6;
					int check_v = pagedAccess.ReadInt32(pos);
					if (check_v < 0) {
						nodeIds.Add(dataPointer);
					} else {
						break;
					}
				}

				// Turn it into a node array,
				int sz = nodeIds.Count;
				NodeId[] lnodeIds = new NodeId[sz];
				for (int i = 0; i < sz; ++i) {
					DataAddress daddr = new DataAddress(blockId, nodeIds[i]);
					lnodeIds[i] = daddr.Value;
				}

				// Read the encoded form into a byte[] array,
				byte[] buf = new byte[dataIdLength];
				content.Seek(dataIdPos, SeekOrigin.Begin);
				content.Read(buf, 0, dataIdLength);

				// Return it,
				return new CompressedNodeSet(lnodeIds, buf);
			} catch (IOException e) {
				// We wrap this IOException around a BlockReadException. This can only
				// indicate a corrupt compressed block file or access to a dataId that
				// is out of range of the nodes stored in this file.
				throw new BlockReadException("IOError reading data from block file", e);
			}
		}

		public void Delete(int dataId) {
			if (dataId < 0 || dataId >= 16384)
				throw new ArgumentException("data_id out of range");

			throw new NotSupportedException();
		}

		public void Flush() {
		}

		public void Close() {
			content.Close();
			content = null;
			pagedAccess = null;
		}

		public long CreateChecksum() {
			throw new NotImplementedException();
		}

		public static void Compress(string sourceFile, string destFile) {
			// Set up the input streams,
			FileStream input;

			try {
				input = new FileStream(sourceFile, FileMode.Open, FileAccess.Read);
			} catch (IOException) {
				string dir = Path.GetDirectoryName(sourceFile);
				string tempSourceFile = Path.Combine(dir, Path.GetTempFileName());
				File.Copy(sourceFile, tempSourceFile, true);
				input = new FileStream(tempSourceFile, FileMode.Open, FileAccess.Read, FileShare.Read);
			}

			BinaryReader reader = new BinaryReader(new BufferedStream(input));

			int[] pos = new int[16384];
			short[] lens = new short[16384];
			bool[] empty = new bool[16384];

			// Read the header,
			int last_header_item = 0;
			for (int n = 0; n < 16384; ++n) {
				pos[n] = reader.ReadInt32();
				lens[n] = reader.ReadInt16();
				if (pos[n] != 0) {
					last_header_item = n + 1;
				} else {
					empty[n] = true;
				}
			}

			input.Close();

			// Create the compressed file,
			if (File.Exists(destFile))
				throw new ApplicationException("Destination file exists: " + destFile);

			// Input file
			int headerSize = (last_header_item + 1) * 6;
			MemoryStream header_out = new MemoryStream(headerSize);
			BinaryWriter dheader_out = new BinaryWriter(header_out);

			{
				using (FileStream outputFile = new FileStream(destFile, FileMode.OpenOrCreate, FileAccess.ReadWrite)) {
					outputFile.SetLength(headerSize);
					outputFile.Seek(headerSize, SeekOrigin.Begin);
				}
			}

			using (BufferedStream fileOutput = new BufferedStream(new FileStream(destFile, FileMode.Append, FileAccess.Write, FileShare.None))) {
				// Input file,
				using (FileStream contents = new FileStream(sourceFile, FileMode.Open, FileAccess.Read, FileShare.None)) {
					// The compression algorithm works as follows;

					MemoryStream bout;

					int fPos = headerSize;

					for (int i = 0; i < last_header_item; ++i) {
						MemoryStream boutToWrite = null;
						int compressStart;
						int compressEnd;
						// For each node,
						int n = i + 1;
						while (true) {
							bout = new MemoryStream(16384);
							DeflateStream compress_out = new DeflateStream(bout, CompressionMode.Compress);
							BinaryWriter compressWriter = new BinaryWriter(compress_out);
							for (int p = i; p < n; ++p) {
								int nodePos = pos[p];
								if (nodePos > 0) {
									short nodeLength = lens[p];
									contents.Seek(nodePos, SeekOrigin.Begin);
									byte[] nodeBuffer = new byte[nodeLength];
									contents.Read(nodeBuffer, 0, nodeBuffer.Length);
									compressWriter.Write(nodeBuffer);
								} else {
									// Make sure to handle the empty node,
									compressWriter.Write((short) 0);
								}
							}

							compressWriter.Flush();

							int compressSize = (int) bout.Length;
							if (n == last_header_item) {
								compressStart = i;
								compressEnd = n;
								boutToWrite = bout;
								break;
							}

							// The compressed size can not go over 4096 bytes, or 24 nodes.
							if (compressSize > 4096 || (n - i) > 24) {
								compressStart = i;
								compressEnd = Math.Max(i + 1, n - 1);
								if (n == i + 1)
									boutToWrite = bout;
								break;
							}
							boutToWrite = bout;
							++n;
						}

						// Write the compressed packet out to the file
						boutToWrite.WriteTo(fileOutput);

						int entry_count = (compressEnd - compressStart);

						dheader_out.Write(fPos);
						dheader_out.Write((short) boutToWrite.Length);
						for (int p = 1; p < entry_count; ++p) {
							dheader_out.Write(-(i + 1));
							dheader_out.Write((short) 0);
						}

						fPos += (int) boutToWrite.Length;

						i = compressEnd - 1;
					}

					// The final header element
					dheader_out.Write((int) 0);
					dheader_out.Write((short) 0);

					dheader_out.Flush();
					dheader_out.Close();
					fileOutput.Flush();
				}
			}

			// Write out the header,
			{
				using (FileStream output_file = new FileStream(destFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 2048, FileOptions.WriteThrough)) {
					output_file.Seek(0, SeekOrigin.Begin);
					byte[] header_buf = header_out.ToArray();
					output_file.Write(header_buf, 0, header_buf.Length);
					// Sync the changes,
					output_file.Flush();
				}
			}

			// Done.
		}
	}
}