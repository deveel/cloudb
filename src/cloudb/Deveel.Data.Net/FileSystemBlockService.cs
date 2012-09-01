using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace Deveel.Data.Net {
	public sealed class FileSystemBlockService : BlockService {
		private readonly string path;
		private readonly List<BlockContainer> compressionAddList = new List<BlockContainer>();
		private CompressionThread compressionThread;

		public FileSystemBlockService(IServiceConnector connector, string path) 
			: base(connector) {
			this.path = path;
		}

		private BlockId ParseFileName(string fnameStr) {
			int p = fnameStr.IndexOf("X");
			if (p == -1)
				throw new FormatException("file name format error: " + fnameStr);

			string h = fnameStr.Substring(0, p);
			string l = fnameStr.Substring(p + 1);

			// Return as a BlockId
			return new BlockId(Convert.ToInt64(h, 16), Convert.ToInt64(l, 16));
		}

		private String FormatFileName(BlockId blockId) {
			long blockIdH = blockId.High;
			long blockIdL = blockId.Low;

			StringBuilder b = new StringBuilder();
			b.AppendFormat("0x{0:x}", blockIdH);
			string l = String.Format("0x{0:x}", blockIdL);
			int pad = 16 - l.Length;
			b.Append("X");
			for (int i = 0; i < pad; ++i) {
				b.Append("0");
			}
			b.Append(l);
			return b.ToString();
		}


		protected override BlockId[] FetchBlockList() {
			string[] dir = Directory.GetFiles(path);
			List<BlockId> blocks = new List<BlockId>(dir.Length);
			foreach (string f in dir) {
				String fnameStr = Path.GetFileName(f);
				if (!fnameStr.Equals("block_server_guid") &&
					!fnameStr.EndsWith(".tempc") &&
					!fnameStr.EndsWith(".tmpc1") &&
					!fnameStr.EndsWith(".tmpc2")) {
					if (fnameStr.EndsWith(".mcd")) {
						fnameStr = fnameStr.Substring(0, fnameStr.Length - 4);
					}
					BlockId blockId = ParseFileName(fnameStr);
					blocks.Add(blockId);
				}
			}

			return blocks.ToArray();
		}

		protected override BlockData GetBlockData(BlockId blockId, int blockType) {
			return new FileBlockData(this, blockId, blockType);
		}

		protected override IBlockStore GetBlockStore(BlockId blockId) {
			// If it's not found in the map,
			// Turn the block id into a filename,
			string blockFname = FormatFileName(blockId);
			string blockFileName = Path.Combine(path, blockFname + ".mcd");
			IBlockStore blockStore;
			if (!File.Exists(blockFileName)) {
				blockFileName = Path.Combine(path, blockFname);
				blockStore = new FileBlockStore(blockId, blockFileName);
			} else {
				blockStore = new CompressedBlockStore(blockId, blockFileName);
			}

			return blockStore;
		}

		protected override void OnBlockLoaded(BlockService.BlockContainer container) {
			// Add the new container to the control list (used by the compression
			// thread).
			lock (compressionAddList) {
				compressionAddList.Add(container);
			}
		}

		protected override void OnStart() {
			// Open the guid file,
			string guidFile = Path.Combine(path, "block_server_guid");
			// If the guid file exists,
			if (File.Exists(guidFile)) {
				// Get the contents,
				StreamReader reader = new StreamReader(guidFile, Encoding.UTF8);
				string firstLine = reader.ReadLine();
				// Set the server guid
				Id = Int64.Parse(firstLine.Trim());
				reader.Close();
			} else {
				// The guid file doesn't exist, so create one now,
				FileStream fileStream = File.Create(guidFile);
				// Create a unique server_guid
				Random r = new Random();
				int v1 = r.Next();
				long v2 = DateTime.Now.Ticks;
				Id = (v2 << 16) ^ (v1 & 0x0FFFFFFF);

				// Write it out to the guid file,
				StreamWriter writer = new StreamWriter(fileStream);
				writer.WriteLine(Id);
				writer.Flush();
				writer.Close();
			}

			// Start the compression thread,
			compressionThread = new CompressionThread(this);

			base.OnStart();
		}

		protected override void OnStop() {
			compressionThread.Finish();
			compressionThread = null;

			lock (compressionAddList) {
				compressionAddList.Clear();
			}
		}

		protected override int DiscoverBlockType(BlockId blockId) {
			// Get the block unit,
			String blockFileName = FormatFileName(blockId);
			int fileType = 1;
			string f = Path.Combine(path, blockFileName);
			if (!File.Exists(f)) {
				fileType = 2;
				f = Path.Combine(path, blockFileName + ".mcd");
			}

			if (!File.Exists(f))
				return -1;

			return fileType;
		}
		
		#region FileBlockData

		class FileBlockData : BlockData {
			private readonly FileSystemBlockService blockService;
			private readonly string fileName;

			public FileBlockData(FileSystemBlockService blockService, BlockId blockId, int fileType) 
				: base(blockId, fileType) {
				this.blockService = blockService;

				fileName = FormatFileName();
			}

			public override bool Exists {
				get { return File.Exists(fileName); }
			}

			private string FormatFileName() {
				String tmpext;
				if (BlockType == 1) {
					tmpext = ".tmpc1";
				} else if (BlockType == 2) {
					tmpext = ".tmpc2";
				} else {
					throw new ApplicationException("Unknown file_type: " + BlockType);
				}

				string blockFname = blockService.FormatFileName(BlockId);
				return Path.Combine(blockService.path, blockFname + tmpext);
			}

			public override Stream OpenRead() {
				return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 2048);
			}

			public override Stream OpenWrite() {
				return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.None, 2048, FileOptions.WriteThrough);
			}

			public override void Complete() {
				string blockFname = blockService.FormatFileName(BlockId);

				// Check the file we are renaming to doesn't exist,
				string fNormal = Path.Combine(blockService.path, blockFname);
				string fCompress = Path.Combine(blockService.path, blockFname + ".mcd");
				if (File.Exists(fNormal) ||
					File.Exists(fCompress)) {
					throw new ApplicationException("Block file exists already");
				}

				// Does exist and is a file,
				// What we will rename the file to,
				if (BlockType == 1) {
					File.Move(fileName, fNormal);
				} else if (BlockType == 2) {
					File.Move(fileName, fCompress);
				} else {
					throw new ApplicationException();
				}
			}
		}

		#endregion

		#region CompressionThread

		private class CompressionThread {
			private readonly FileSystemBlockService service;
			private readonly Thread thread;
			private bool finished;
			private bool hasFinished;

			public CompressionThread(FileSystemBlockService service) {
				this.service = service;

				thread = new Thread(Execute);
				thread.IsBackground = true;
				thread.Name = "BlockService::Compression";
				thread.Start();
			}

			public void Finish() {
				lock (this) {
					finished = true;
					while (hasFinished == false) {
						try {
							Monitor.PulseAll(this);
							Monitor.Wait(this);
						} catch (ThreadInterruptedException) {
						}
					}
				}
			}


			private void Execute() {
				try {
					lock (this) {

						// Wait 2 seconds,
						Monitor.Wait(2000);

						// Any new block containers added, we need to process,
						List<BlockContainer> newItems = new List<BlockContainer>();

						while (true) {

							lock (service.compressionAddList) {
								newItems.AddRange(service.compressionAddList);
								service.compressionAddList.Clear();
							}

							// Sort the container list,
							newItems.Sort();
							newItems.Reverse();

							for (int i = newItems.Count - 1; i >= 0; i--) {
								BlockContainer container = newItems[i];

								// If it's already compressed, remove it from the list
								if (container.IsCompressed) {
									newItems.RemoveAt(i);
								}
									// Don't compress if written to less than 3 minutes ago,
									// and we confirm it can be compressed,
								else if (service.IsKnownStaticBlock(container)) {

									FileBlockStore mblockStore = (FileBlockStore)container.Store;
									string sourcef = mblockStore.FileName;
									string destf = Path.Combine(Path.GetDirectoryName(sourcef),
																Path.GetFileName(sourcef) + ".tempc");
									try {
										File.Delete(destf);
										service.Logger.Info(String.Format("Compressing block: {0}", container.Id));
										service.Logger.Info(String.Format("Current block size = {0}", sourcef.Length));

										// Compress the file,
										CompressedBlockStore.Compress(sourcef, destf);

										// Rename the file,
										string compressedf = Path.Combine(Path.GetDirectoryName(sourcef),
																		  Path.GetFileName(sourcef) + ".mcd");
										File.Move(destf, compressedf);

										// Switch the block container,
										container.ChangeStore(new CompressedBlockStore(container.Id, compressedf));

										service.Logger.Info(String.Format("Compression of block {0} finished.", container.Id));
										service.Logger.Info(String.Format("Compressed block size = {0}", compressedf.Length));
										// Wait a little bit and delete the original file,
										if (finished) {
											hasFinished = true;
											Monitor.PulseAll(this);
											return;
										}

										Monitor.Wait(this, 1000);

										// Delete the file after 5 minutes,
										new Timer(FileDelete, sourcef, 5 * 60 * 1000, Timeout.Infinite);

										// Remove it from the new_items list
										newItems.RemoveAt(i);
									} catch (IOException e) {
										service.Logger.Error("IO Error in compression thread", e);
									}
								}

								if (finished) {
									hasFinished = true;
									Monitor.PulseAll(this);
									return;
								}

								Monitor.Wait(this, 200);
							}


							if (finished) {
								hasFinished = true;
								Monitor.PulseAll(this);
								return;
							}

							Monitor.Wait(this, 3000);
						}
					}
				} catch (ThreadInterruptedException) {
					// ThreadInterruptedException causes the thread to end,
				}

			}

			private void FileDelete(object state) {
				string sourcef = (string)state;
				service.Logger.Info("Deleting file {0}", sourcef);
				File.Delete(sourcef);
			}
		}

		#endregion

	}
}