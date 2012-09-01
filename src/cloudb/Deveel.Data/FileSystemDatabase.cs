using System;
using System.IO;

using Deveel.Data.Diagnostics;
using Deveel.Data.Store;

namespace Deveel.Data {
	public class FileSystemDatabase : IDatabase {
		private readonly string path;
		private readonly Logger logger;

		private LoggingBufferManager bufferManager;
		private JournalledFileStore fileStore;
		private StoreTreeSystem treeSystem;

		private bool databaseStarted;

		private long fileRolloverSize;
		private int pageSize;
		private int maxPageCount;
		private int branchNodeSize;
		private int leafNodeSize;
		private long heapNodeCacheSize;
		private long branchNodeCacheSize;

		private readonly Object lockObject = new Object();
		private readonly Object commitLock = new Object();

		public FileSystemDatabase(string path) {
			this.path = path;
			logger = Logger.GetLogger("FileSystemDatabase");

			SetDefaultValues();
		}

		private void SetDefaultValues() {
			lock (lockObject) {
				fileRolloverSize = 512*1024*1024;
				pageSize = 8*1024;
				maxPageCount = 1024;
				branchNodeSize = 16;
				leafNodeSize = 4010;
				heapNodeCacheSize = 14*1024*1024;
				branchNodeCacheSize = 2*1024*1024;
			}
		}

		public int PageSize {
			set {
				lock (lockObject) {
					pageSize = value;
				}
			}
			get {
				lock (lockObject) {
					return pageSize;
				}
			}
		}

		public int MaxPageCount {
			set {
				lock (lockObject) {
					maxPageCount = value;
				}
			}
			get {
				lock (lockObject) {
					return maxPageCount;
				}
			}
		}

		public long FileRolloverSize {
			set {
				lock (lockObject) {
					fileRolloverSize = value;
				}
			}
			get {
				lock (lockObject) {
					return fileRolloverSize;
				}
			}
		}

		public int BranchNodeSize {
			set {
				lock (lockObject) {
					branchNodeSize = value;
				}
			}
			get {
				lock (lockObject) {
					return branchNodeSize;
				}
			}
		}

		public int LeafNodeSize {
			set {
				lock (lockObject) {
					leafNodeSize = value;
				}
			}
			get {
				lock (lockObject) {
					return leafNodeSize;
				}
			}
		}

		public long HeapNodeCacheSize {
			set {
				lock (lockObject) {
					heapNodeCacheSize = value;
				}
			}
			get {
				lock (lockObject) {
					return heapNodeCacheSize;
				}
			}
		}

		public long BranchNodeCacheSize {
			get {
				lock (lockObject) {
					return branchNodeCacheSize;
				}
			}
			set {
				lock (lockObject) {
					branchNodeCacheSize = value;
				}
			}
		}

		public ITreeSystem TreeSystem {
			get { return treeSystem; }
		}

		public bool Start() {
			lock (lockObject) {
				// We can't start a database that is already started,
				if (databaseStarted || treeSystem != null)
					return false;

				// Make a data.koi file with a single TreeSystem structure mapped into it
				const string fileExt = "cdb";
				const string dbFileName = "data";

				bufferManager = new LoggingBufferManager(path, path, false, maxPageCount, pageSize, fileExt, fileRolloverSize,
				                                          logger, true);
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
					IAreaWriter awriter = fileStore.CreateArea(128);
					awriter.WriteInt4(0x0101); // The version value
					awriter.WriteInt8(treePointer);
					awriter.WriteInt4(branchNodeSize);
					awriter.WriteInt4(leafNodeSize);
					awriter.Finish();
					long dummy = awriter.Id;
					IMutableArea harea = fileStore.GetMutableArea(-1);
					harea.WriteInt4(0x092BA001); // The magic value
					harea.WriteInt8(awriter.Id);
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
				databaseStarted = true;
				return true;
			}
		}

		public void Stop() {
			// Check point before we stop
			CheckPoint();

			lock (lockObject) {
				// We can't stop a database that hasn't started
				if (databaseStarted == false || treeSystem == null)
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
				databaseStarted = false;
			}
		}

		public TreeReportNode CreateDiagnosticGraph() {
			return treeSystem.CreateDiagnosticGraph();
		}


		public ITransaction CreateTransaction() {
			return treeSystem.CreateTransaction();
		}

		public void Publish(ITransaction transaction) {
			lock (commitLock) {
				treeSystem.Commit(transaction);
			}
		}

		public void Dispose(ITransaction transaction) {
			treeSystem.Dispose(transaction);
		}

		public void CheckPoint() {
			treeSystem.CheckPoint();
		}
	}
}