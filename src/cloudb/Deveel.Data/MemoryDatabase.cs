using System;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class MemoryDatabase : IDatabase {
		private HeapStore store;
		private StoreTreeSystem treeSystem;
		private int branchNodeSize;
		private int leafNodeSize;
		private long heapNodeCacheSize;
		private long branchNodeCacheSize;

		private bool disposed;

		private readonly object lockObject = new object();
		private bool databaseStarted;


		public MemoryDatabase(int hashSize) {
			store = new HeapStore(hashSize);
			branchNodeSize = 16;
			leafNodeSize = 4010;
			heapNodeCacheSize = 14 * 1024 * 1024;
			branchNodeCacheSize = 2 * 1024 * 1024;
		}

		~MemoryDatabase() {
			Dispose(false);
		}

		public int BranchNodeSize {
			get {
				lock (lockObject) {
					return branchNodeSize;
				}
			}
			set {
				lock (lockObject) {
					branchNodeSize = value;
				}
			}
		}

		public int LeafNodeSize {
			get {
				lock (lockObject) {
					return leafNodeSize;
				}
			}
			set {
				lock (lockObject) {
					leafNodeSize = value;
				}
			}
		}

		public long HeapNodeCacheSize {
			get {
				lock (lockObject) {
					return heapNodeCacheSize;
				}
			}
			set {
				lock (lockObject) {
					heapNodeCacheSize = value;
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

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (databaseStarted)
						Stop();
				}

				disposed = true;
			}
 		}

		public bool Start() {
			lock (lockObject) {
				treeSystem = new StoreTreeSystem(store, branchNodeSize, leafNodeSize, heapNodeCacheSize, branchNodeCacheSize);
				treeSystem.Create();
				treeSystem.CheckPoint();

				// The actual database
				StoreTreeSystem treeStore;

				// Get the header area
				IArea headerArea = store.GetArea(-1);
				int magicValue = headerArea.ReadInt4();
				// If header area magic value is zero, then we assume this is a brand
				// new database and initialize it with the configuration information
				// given.
				if (magicValue == 0) {
					// Create a tree store inside the file store,
					treeStore = new StoreTreeSystem(store, branchNodeSize, leafNodeSize, heapNodeCacheSize,
													branchNodeCacheSize);
					// Create the tree and returns a pointer to the tree,
					long treePointer = treeStore.Create();

					// Create an area object with state information about the tree
					IAreaWriter awriter = store.CreateArea(128);
					awriter.WriteInt4(0x0101); // The version value
					awriter.WriteInt8(treePointer);
					awriter.WriteInt4(branchNodeSize);
					awriter.WriteInt4(leafNodeSize);
					awriter.Finish();
					long dummy = awriter.Id;
					IMutableArea harea = store.GetMutableArea(-1);
					harea.WriteInt4(0x092BA001); // The magic value
					harea.WriteInt8(awriter.Id);
					harea.CheckOut();
				} else if (magicValue == 0x092BA001) {
					long apointer = headerArea.ReadInt8();
					// The area that contains configuration details,
					IArea initArea = store.GetArea(apointer);
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
					treeStore = new StoreTreeSystem(store, ibranchNodeSize, ileafNodeSize, heapNodeCacheSize,
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

		public void Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		public void Stop() {
			CheckPoint();

			lock (lockObject) {
				// We can't stop a database that hasn't started
				if (databaseStarted == false || treeSystem == null)
					return;

				// Offer up all the internal objects to the GC
				store = null;

				// Clear the internal state
				treeSystem = null;
				databaseStarted = false;
			}
		}

		public ITransaction CreateTransaction() {
			return treeSystem.CreateTransaction();
		}

		public void Publish(ITransaction transaction) {
			treeSystem.Commit(transaction);
		}

		public void Dispose(ITransaction transaction) {
			treeSystem.Dispose(transaction);
		}

		public void CheckPoint() {
			treeSystem.CheckPoint();
		}
	}
}