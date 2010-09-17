using System;
using System.IO;

using Deveel.Data.Diagnostics;
using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class FileSystemDatabase : IDatabase {
		private readonly string path;
		private DefaultLogger debug;
		private LoggingBufferManager bufferManager;
		private JournalledFileStore fileStore;
		private StoreTreeSystem treeSystem;
		private bool started;
		private long fileRolloverSize;
		private int pageSize;
		private int maxPageCount;
		private int branchNodeSize;
		private int leafNodeSize;
		private long heapNodeCacheSize;
		private long branchNodeCacheSize;

		private readonly object lockObject = new object();
		private readonly object commitLock = new object();

		public FileSystemDatabase(string path) {
			this.path = path;
			debug = new DefaultLogger();
			debug.SetDebugLevel(1000000);

			// set the defaults ...
			fileRolloverSize = 512 * 1024 * 1024;
			pageSize = 8 * 1024;
			maxPageCount = 1024;
			branchNodeSize = 16;
			leafNodeSize = 4010;
			heapNodeCacheSize = 14 * 1024 * 1024;
			branchNodeCacheSize = 2 * 1024 * 1024;
		}

		public int PageSize {
			get {
				lock(lockObject) {
					return pageSize;
				}
			}
			set {
				lock(lockObject) {
					pageSize = value;
				}
			}
		}

		public int MaxPageCount {
			get {
				lock(lockObject) {
					return maxPageCount;
				}
			}
			set {
				lock(lockObject) {
					maxPageCount = value;
				}
			}
		}

		public long FileRolloverSize {
			get {
				lock(lockObject) {
					return fileRolloverSize;
				}
			}
			set {
				lock(lockObject) {
					fileRolloverSize = value;
				}
			}
		}

		public int BranchNodeSize {
			get {
				lock(lockObject) {
					return branchNodeSize;
				}
			}
			set {
				lock(lockObject) {
					branchNodeSize = value;
				}
			}
		}

		public int LeafNodeSize {
			get {
				lock(lockObject) {
					return leafNodeSize;
				}
			}
			set {
				lock(lockObject) {
					leafNodeSize = value;
				}
			}
		}

		public long HeapNodeCacheSize {
			get {
				lock(lockObject) {
					return heapNodeCacheSize;
				}
			}
			set {
				lock(lockObject) {
					heapNodeCacheSize = value;
				}
			}
		}

		public long BranchNodeCacheSize {
			get {
				lock(lockObject) {
					return branchNodeCacheSize;
				}
			}
			set {
				lock(lockObject) {
					branchNodeCacheSize = value;
				}
			}
		}

		public bool Start() {
			lock (lockObject) {
				// We can't start a database that is already started,
				if (started || treeSystem != null)
					return false;

				// Make a data.db file with a single TreeSystem structure mapped into it
				const string fileExt = "db";
				const string dbFileName = "data";

				debug = new DefaultLogger();
				debug.SetDebugLevel(1000000);
				bufferManager = new LoggingBufferManager(path, path, false, maxPageCount, pageSize, fileExt, fileRolloverSize,
				                                          new Logger("data", debug), true);
				bufferManager.Start();

				// The backing store
				fileStore = new JournalledFileStore(dbFileName, bufferManager, false);
				fileStore.Open();

				// The actual database
				StoreTreeSystem treeStore;

				// Get the header area
				IArea headerArea = fileStore.GetArea(-1);
				int magicValue = headerArea.ReadInt4();
				// If header area magic value is zero, then we assume this is a brand
				// new database and initialize it with the configuration information
				// given.
				if (magicValue == 0) {
					// Create a tree store inside the file store,
					treeStore = new StoreTreeSystem(fileStore, branchNodeSize, leafNodeSize, heapNodeCacheSize,
					                                 branchNodeCacheSize);
					// Create the tree and returns a pointer to the tree,
					long treePointer = treeStore.Create();

					// Create an area object with state information about the tree
					IAreaWriter area = fileStore.CreateArea(128);
					area.WriteInt4(0x0101);    // The version value
					area.WriteInt8(treePointer);
					area.WriteInt4(branchNodeSize);
					area.WriteInt4(leafNodeSize);
					area.Finish();
					long areaId = area.Id;
					IMutableArea harea = fileStore.GetMutableArea(-1);
					harea.WriteInt4(0x092BA001);  // The magic value
					harea.WriteInt8(areaId);
					harea.CheckOut();
				} else if (magicValue == 0x092BA001) {
					long apointer = headerArea.ReadInt8();
					// The area that contains configuration details,
					IArea initArea = fileStore.GetArea(apointer);
					int version = initArea.ReadInt4();
					if (version != 0x0101)
						throw new IOException("Unknown version in tree initialization area");

					// Read the pointer to the tree store
					long treePointer = initArea.ReadInt8();
					// Read the branch and leaf node sizes as set when the database was
					// created.
					int ibranchNodeSize = initArea.ReadInt4();
					int ileafNodeSize = initArea.ReadInt4();

					// Create the tree store
					treeStore = new StoreTreeSystem(fileStore, ibranchNodeSize, ileafNodeSize, heapNodeCacheSize,
					                                branchNodeCacheSize);
					// Initialize the tree
					treeStore.Init(treePointer);
				} else {
					throw new IOException("Data is corrupt, invalid magic value in store");
				}

				// Set the point of the tree store
				treeStore.CheckPoint();

				// Set up final internal state and return true
				treeSystem = treeStore;
				started = true;
				return true;
			}
		}

		public void Stop() {
			// Check point before we stop
			CheckPoint();

			lock (lockObject) {
				// We can't stop a database that hasn't started
				if (!started || treeSystem == null)
					return;

				// Close the store
				fileStore.Close();
				// Stop the buffer manager
				bufferManager.Stop();
				// Offer up all the internal objects to the GC
				bufferManager = null;
				fileStore = null;

				// Clear the internal state
				treeSystem = null;
				started = false;

			}
		}

		#region Implementation of IDatabase

		public ITransaction CreateTransaction() {
			return treeSystem.CreateTransaction();
		}

		public void Publish(ITransaction transaction) {
			lock(commitLock) {
				treeSystem.Commit(transaction);
			}
		}

		public void Dispose(ITransaction transaction) {
			treeSystem.Dispose(transaction);
		}

		public void CheckPoint() {
			treeSystem.CheckPoint();
		}

		#endregion

		public TreeGraph CreateGraph() {
			return treeSystem.CreateGraph();
		}

		public static void CopyData(ITransaction source, ITransaction destination) {
			// The transaction in this object,
			TreeSystemTransaction sourcet = (TreeSystemTransaction)source;
			foreach(Key key in sourcet.Keys) {
				// Get the source and destination files
				DataFile sourceFile = sourcet.GetFile(key, FileAccess.ReadWrite);
				DataFile destFile = destination.GetFile(key, FileAccess.Write);
				// Copy the data
				sourceFile.CopyTo(destFile, sourceFile.Length);
			}
		}
	}
}