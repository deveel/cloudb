using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Deveel.Data.Net {
	public sealed class FileSystemBlockServer : BlockServer {
		private readonly string path;
		
		private bool compressFinished;
		private bool hasCompressFinished;
		private readonly List<BlockContainer> compressionAddList = new List<BlockContainer>();
		private readonly object compressLock = new object();
		private readonly Thread compressionThread;
		
		private Timer fileDeleteTimer;
		
		public FileSystemBlockServer(IServiceConnector connector, string path)
			: base(connector) {
			this.path = path;
			compressionThread = new Thread(Compress);
			compressionThread.IsBackground = true;
		}
		
		private void FileDelete(object state) {
			string fileName = (string)state;
			
			//TODO: INFO log ...
			
			File.Delete(fileName);
		}
		
		private void Compress() {
			try {
				lock(compressLock) {
					// Wait 2 seconds,
					Monitor.Wait(compressLock, 2000);

					// Any new block containers added, we need to process,
					List<BlockContainer> new_items = new List<BlockContainer>();
					while (true) {
						lock(compressionAddList) {
							foreach(BlockContainer container in compressionAddList)
								new_items.Add(container);
							compressionAddList.Clear();
						}

						// Sort the container list,
						new_items.Sort();

						for (int i = new_items.Count - 1; i >= 0; i--) {
							BlockContainer container = new_items[i];

							// If it's already compressed, remove it from the list
							if (container.BlockStore is CompressedBlockStore) {
								new_items.RemoveAt(i);
							} else if (container.LastWriteTime <
							           DateTime.Now.AddMilliseconds(-(3*60*1000))) {

								FileBlockStore mblock_store = (FileBlockStore) container.BlockStore;
								string sourcef = mblock_store.FileName;
								string destf = Path.Combine(Path.GetDirectoryName(sourcef),
									                        Path.GetFileNameWithoutExtension(sourcef) + ".tempc");
								try {
									File.Delete(destf);
									
									//TODO: INFO log ...

									// Compress the file,
									CompressedBlockStore.Compress(sourcef, destf);
									// Rename the file,
									string compressedf = Path.Combine(Path.GetDirectoryName(sourcef),
									                                  Path.GetFileNameWithoutExtension(sourcef) + ".mcd");
									File.Move(destf, compressedf);

									// Switch the block container,
									container.ChangeStore(new CompressedBlockStore(container.BlockId, compressedf));

									//TODO: INFO log ...
									
									// Wait a little bit and delete the original file,
									if (compressFinished) {
										hasCompressFinished = true;
										Monitor.PulseAll(compressLock);
										return;
									}
									
									Monitor.Wait(compressLock, 1000);

									// Delete the file after 5 minutes,
									fileDeleteTimer = new Timer(FileDelete, sourcef, 5*60*1000, 5*60*1000);

									// Remove it from the new_items list
									new_items.RemoveAt(i);
								} catch(IOException e) {
									//TODO: ERROR log ...
								}
							}

							if (compressFinished) {
								hasCompressFinished = true;
								Monitor.PulseAll(this);
								return;
							}
							Monitor.Wait(this, 200);

						}

						if (compressFinished) {
							hasCompressFinished = true;
							Monitor.PulseAll(compressLock);
							return;
						}
						Monitor.Wait(compressLock, 3000);
					}
				}
			} catch(ThreadInterruptedException) {
				// ThreadInterruptedException causes the thread to end,
			}
		}
		
		private void FinishCompress() {
			lock (compressLock) {
				compressFinished = true;
				while (!hasCompressFinished) {
					try {
						Monitor.PulseAll(compressLock);
						Monitor.Wait(compressLock);
					} catch (ThreadInterruptedException) {
					}
				}
			}
		}
		
		protected override void OnInit() {
			// Open the guid file,
			string guidFile = Path.Combine(path, "block_server_guid");
			// If the guid file exists,
			if (File.Exists(guidFile)) {
				// Get the contents,
				using(StreamReader reader = new StreamReader(guidFile)) {
					string line = reader.ReadLine();
					// Set the server guid
					SetGuid(Int64.Parse(line.Trim()));
				}
			} else {
				// The guid file doesn't exist, so create one now,
				Stream fileStream;
				try {
					fileStream = File.Create(guidFile);
				} catch (Exception e) {
					throw new ApplicationException("Unable to create guid server file", e);
				}

				// Create a unique server_guid
				Random r = new Random();
				int v1 = r.Next();
				long v2 = DateTime.Now.Ticks;
				long guid = (v2 << 16) ^ (v1 & 0x0FFFFFFF);

				// Write it out to the guid file,
				using (StreamWriter writer = new StreamWriter(fileStream)) {
					writer.WriteLine(guid);
					writer.Flush();
				}
				
				SetGuid(guid);
			}
			
			compressionThread.Start();
		}
		
		protected override BlockServer.BlockContainer LoadBlock(long blockId) {
			throw new NotImplementedException();
		}
		
		protected override long[] ListBlocks() {
			throw new NotImplementedException();
		}
		
		protected override void OnCompleteBlockWrite(long blockId, int storeType) {
			base.OnCompleteBlockWrite(blockId, storeType);
		}
		
		protected override void OnWriteBlockPart(long blockId, long pos, int storeType, byte[] buffer, int length) {
			base.OnWriteBlockPart(blockId, pos, storeType, buffer, length);
		}
		
		protected override void OnDispose(bool disposing) {
			base.OnDispose(disposing);
		}
	}
}