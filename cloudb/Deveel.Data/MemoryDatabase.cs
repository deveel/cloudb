using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class MemoryDatabase : IDatabase {
		private readonly HeapStore store;
		private StoreTreeSystem treeSystem;
		private int branchNodeSize;
		private int leafNodeSize;
		private long heapNodeCacheSize;
		private long branchNodeCacheSize;
		private readonly object lockObject = new object();


		public MemoryDatabase(int hashSize) {
			store = new HeapStore(hashSize);
			branchNodeSize = 16;
			leafNodeSize = 4010;
			heapNodeCacheSize = 14 * 1024 * 1024;
			branchNodeCacheSize = 2 * 1024 * 1024;
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

		public void Start() {
			lock (lockObject) {
				treeSystem = new StoreTreeSystem(store, branchNodeSize, leafNodeSize, heapNodeCacheSize, branchNodeCacheSize);
				treeSystem.Create();
				treeSystem.CheckPoint();
			}
		}

		public void Stop() {
			CheckPoint();
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