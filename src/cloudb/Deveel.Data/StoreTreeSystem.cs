using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using Deveel.Data.Caching;
using Deveel.Data.Store;

namespace Deveel.Data {
	internal class StoreTreeSystem : ITreeSystem {
		private const short StoreLeafType = 0x019EC;
		private const short StoreBranchType = 0x022EB;

		private volatile ErrorStateException critical_stop_error = null;

		private readonly List<VersionInfo> versions;

		private readonly int maxBranchSize;
		private readonly int maxLeafByteSize;
		private readonly long nodeHeapMaxSize;

		private readonly IStore nodeStore;

		private readonly Cache branchCache;

		private bool initialized;
		private long headerId;

		private readonly Object referenceCountLock = new Object();

		public StoreTreeSystem(IStore nodeStore, int maxBranchChildren, int maxLeafSize, long nodeMaxCacheMemory,
		                       long branchCacheMemory) {
			maxBranchSize = maxBranchChildren;
			maxLeafByteSize = maxLeafSize;
			this.nodeStore = nodeStore;
			nodeHeapMaxSize = nodeMaxCacheMemory;
			versions = new List<VersionInfo>();

			// Allocate some values for the branch cache,
			long branchSizeEstimate = (maxBranchChildren*24) + 64;
			// The number of elements in the branch cache
			int branchCacheElements = (int) (branchCacheMemory/branchSizeEstimate);
			// Find a close prime to this
			int branchPrime = Cache.ClosestPrime(branchCacheElements + 20);
			// Allocate the cache
			branchCache = new MemoryCache(branchPrime, branchCacheElements, 20);

			initialized = false;
		}

		public int MaxBranchSize {
			get { return maxBranchSize; }
		}

		public int MaxLeafByteSize {
			get { return maxLeafByteSize; }
		}

		public long NodeHeapMaxSize {
			get { return nodeHeapMaxSize; }
		}

		public bool NotifyNodeChanged {
			get { return true; }
		}

		private NodeId FromInt64StoreAddress(long value) {
			return new NodeId(0, value);
		}

		private long ToInt64StoreAddress(NodeId nodeId) {
			return nodeId.Low;
		}

		private TreeSystemTransaction CreateSnapshot(VersionInfo vinfo) {
			return new TreeSystemTransaction(this, vinfo.VersionId, vinfo.RootNodeId, false);
		}

		private long WriteSingleVersionInfo(long versionId, NodeId rootNodeId, List<NodeId> deletedRefs) {
			int deletedRefCount = deletedRefs.Count;

			// Write the version info and the deleted refs to a new area,
			IAreaWriter writer = nodeStore.CreateArea(4 + 4 + 8 + 8 + 8 + 4 + (deletedRefCount*16));
			writer.WriteInt4(0x04EA23);
			writer.WriteInt4(1);
			writer.WriteInt8(versionId);
			writer.WriteInt8(rootNodeId.High);
			writer.WriteInt8(rootNodeId.Low);

			writer.WriteInt4(deletedRefCount);
			foreach (NodeId deletedNode in deletedRefs) {
				writer.WriteInt8(deletedNode.High);
				writer.WriteInt8(deletedNode.Low);
			}

			writer.Finish();

			return writer.Id;
		}

		private VersionInfo ReadSingleVersionInfo(long versionRef) {
			IArea verArea = nodeStore.GetArea(versionRef);
			int magic = verArea.ReadInt4();
			int version = verArea.ReadInt4();
			long versionId = verArea.ReadInt8();
			long rnrHigh = verArea.ReadInt8();
			long rnrLow = verArea.ReadInt8();
			NodeId rootNodeId = new NodeId(rnrHigh, rnrLow);

			if (magic != 0x04EA23)
				throw new IOException("Incorrect magic value 0x04EA23");
			if (version < 1)
				throw new IOException("Version incorrect.");

			return new VersionInfo(versionId, rootNodeId, versionRef);
		}

		private long WriteVersionsList(long versionId, TreeSystemTransaction tran) {
			lock (this) {
				// Write the version info and the deleted refs to a new area,
				NodeId rootNodeId = tran.RootNodeId;
				if (rootNodeId.IsInMemory)
					throw new ApplicationException("Assertion failed, root_node is on heap.");

				// Get the list of all nodes deleted in the transaction
				List<NodeId> deletedRefs = tran.NodeDeletes;
					// Sort it
					deletedRefs.Sort();
					// Check for any duplicate entries (we shouldn't double delete stuff).
					for (int i = 1; i < deletedRefs.Count; ++i) {
						if (deletedRefs[i - 1].Equals(deletedRefs[i])) {
							// Oops, duplicated delete
							throw new ApplicationException("PRAGMATIC_CHECK failed: duplicate records in delete list.");
						}
					}


				long theVersionId = WriteSingleVersionInfo(versionId, rootNodeId, deletedRefs);

				// Now update the version list by copying the list and adding the new ref
				// to the end.

				// Get the current version list
				IMutableArea headerArea = nodeStore.GetMutableArea(headerId);
				headerArea.Position = 8;

				long versionListId = headerArea.ReadInt8();

				// Read information from the old version info,
				IArea versionListArea = nodeStore.GetArea(versionListId);
				versionListArea.ReadInt4(); // The magic
				int versionCount = versionListArea.ReadInt4();

				// Create a new list,
				IAreaWriter newVersionList = nodeStore.CreateArea(8 + (8*(versionCount + 1)));
				newVersionList.WriteInt4(0x01433);
				newVersionList.WriteInt4(versionCount + 1);
				for (int i = 0; i < versionCount; ++i) {
					newVersionList.WriteInt8(versionListArea.ReadInt8());
				}
				newVersionList.WriteInt8(theVersionId);
				newVersionList.Finish();

				// Write the new area to the header,
				headerArea.Position = 8;
				headerArea.WriteInt8(newVersionList.Id);

				// Delete the old version list Area,
				nodeStore.DeleteArea(versionListId);

				// Done,
				return theVersionId;
			}
		}

		private void DisposeOldVersions() {
			List<object> disposeList = new List<object>();
			lock (versions) {
				// size - 1 because we don't want to delete the very last version,
				int sz = versions.Count - 1;
				bool foundLockedEntry = false;
				for (int i = 0; i < sz && foundLockedEntry == false; ++i) {
					VersionInfo vinfo = versions[i];
					// If this version isn't locked,
					if (vinfo.NotLocked) {
						// Add to the dispose list
						disposeList.Add(vinfo);
						// And delete from the versions list,
						versions.RemoveAt(i);
						--sz;
						--i;
					} else {
						// If it is locked, we exit the loop
						foundLockedEntry = true;
					}
				}
			}

			// If there are entries to dispose?
			if (disposeList.Count > 0) {
				// We synchronize here to ensure the versions list can't be modified by
				// a commit operation while we are disposing this.
				lock (this) {
					// Run within a write lock on the store
					try {
						nodeStore.LockForWrite();

						// First we write out a modified version header minus the versions we
						// are to delete,

						// Get the current version list
						IMutableArea headerArea = nodeStore.GetMutableArea(headerId);
						headerArea.Position = 8;

						long versionListId = headerArea.ReadInt8();

						// Read information from the old version info,
						IArea versionListArea = nodeStore.GetArea(versionListId);
						versionListArea.ReadInt4(); // The magic

						int versionCount = versionListArea.ReadInt4();


						int newVersionCount = versionCount - disposeList.Count;
						// Create a new list,
						IAreaWriter newVersionList = nodeStore.CreateArea(8 + (8*newVersionCount));
						newVersionList.WriteInt4(0x01433);
						newVersionList.WriteInt4(newVersionCount);
						// Skip the versions we are deleting,
						for (int i = 0; i < disposeList.Count; ++i) {
							versionListArea.ReadInt8();
						}
						// Now copy the list from the new point
						for (int i = 0; i < newVersionCount; ++i) {
							newVersionList.WriteInt8(versionListArea.ReadInt8());
						}
						newVersionList.Finish();

						// Write the new area to the header,
						headerArea.Position = 8;
						headerArea.WriteInt8(newVersionList.Id);

						// Delete the old version list Area,
						nodeStore.DeleteArea(versionListId);

						// Dispose the version info,
						int sz = disposeList.Count;
						for (int i = 0; i < sz; ++i) {
							VersionInfo vinfo = (VersionInfo) disposeList[i];
							long vRef = vinfo.VersionInfoRef;
							IArea versionArea = nodeStore.GetArea(vRef);
							int magic = versionArea.ReadInt4();
							int rev = versionArea.ReadInt4();
							// Check the magic,
							if (magic != 0x04EA23)
								throw new ApplicationException("Magic value for version area is incorrect.");

							long verId = versionArea.ReadInt8();
							long nrnHigh = versionArea.ReadInt8();
							long nrnLow = versionArea.ReadInt8();

							int nodeCount = versionArea.ReadInt4();
							// For each node,
							for (int n = 0; n < nodeCount; ++n) {
								// Read the next area
								long drnHigh = versionArea.ReadInt8();
								long drnLow = versionArea.ReadInt8();
								NodeId delNodeId = new NodeId(drnHigh, drnLow);
								// Cleanly disposes the node
								DoDisposeNode(delNodeId);
							}

							// Delete the node header,
							nodeStore.DeleteArea(vRef);
						}
					} finally {
						nodeStore.UnlockForWrite();
					}
				}
			}
		}

		private void UnlockTransaction(long versionId) {
			bool done = false;
			lock (versions) {
				int sz = versions.Count;
				for (int i = sz - 1; i >= 0 && done == false; --i) {
					VersionInfo vinfo = versions[i];
					if (vinfo.VersionId == versionId) {
						// Unlock this version,
						vinfo.Unlock();
						// And finish,
						done = true;
					}
				}
			}
			if (!done) {
				throw new ApplicationException("Unable to find version to unlock: " + versionId);
			}
		}

		private void DoDisposeNode(NodeId id) {
			// If the node is a special node, then we don't dispose it
			if (id.IsSpecial)
				return;

			// Is 'id' a leaf node?
			IMutableArea nodeArea = nodeStore.GetMutableArea(ToInt64StoreAddress(id));
			// Are we a leaf?
			nodeArea.Position = 0;
			int nodeType = nodeArea.ReadInt2();
			if (nodeType == StoreLeafType) {
				// Yes, get its reference_count,
				lock (referenceCountLock) {
					nodeArea.Position = 4;
					int refCount = nodeArea.ReadInt4();
					// If the reference_count is >1 then decrement it and return
					if (refCount > 1) {
						nodeArea.Position = 4;
						nodeArea.WriteInt4(refCount - 1);
						return;
					}
				}
			} else if (nodeType != StoreBranchType) {
				// Has to be a branch type, otherwise failure
				throw new IOException("Unknown node type.");
			}
			// 'id' is a none leaf branch or its reference count is 1, so delete the
			// area.

			// NOTE, we delete from the cache first before we delete the area
			//   because the deleted area may be reclaimed immediately and deleting
			//   from the cache after may be too late.

			// Delete from the cache because the given ref may be recycled for a new
			// node at some point.
			lock (branchCache) {
				branchCache.Remove(id);
			}

			// Delete the area
			nodeStore.DeleteArea(ToInt64StoreAddress(id));
		}

		internal static ITreeNode SpecialStaticNode(NodeId nodeId) {
			return nodeId.CreateSpecialTreeNode();
		}


		private ITreeNode FetchNode(NodeId nodeId) {
			// Is it a special static node?
			if (nodeId.IsSpecial) {
				return SpecialStaticNode(nodeId);
			}

			// Is this a branch node in the cache?
			NodeId cacheKey = nodeId;
			TreeBranch branch;
			lock (branchCache) {
				branch = (TreeBranch) branchCache.Get(cacheKey);
				if (branch != null) {
					return branch;
				}
			}

			// Not found in the cache, so fetch the area from the backing store and
			// create the node type.

			// Get the area for the node
			IArea nodeArea = nodeStore.GetArea(ToInt64StoreAddress(nodeId));
			// Wrap around a buffered BinaryRader for reading values from the store.
			BinaryReader input = new BinaryReader(new AreaInputStream(nodeArea, 256));

			short nodeType = input.ReadInt16();
			// Is the node type a leaf node?
			if (nodeType == StoreLeafType) {
				// Read the key
				input.ReadInt16(); // version
				input.ReadInt32(); // reference count
				int leafSize = input.ReadInt32();

				// Return a leaf that's mapped to the data in the store
				nodeArea.Position = 0;
				return new AreaTreeLeaf(nodeId, leafSize, nodeArea);
			}
				// Is the node type a branch node?
			if (nodeType == StoreBranchType) {
				// Note that the entire branch is loaded into memory now,
				input.ReadInt16(); // version
				int childDataSize = input.ReadInt32();
				long[] dataArr = new long[childDataSize];
				for (int i = 0; i < childDataSize; ++i) {
					dataArr[i] = input.ReadInt64();
				}
				branch = new TreeBranch(nodeId, dataArr, childDataSize);
				// Put this branch in the cache,
				lock (branchCache) {
					branchCache.Set(cacheKey, branch);
					// And return the branch
					return branch;
				}
			}

			throw new ApplicationException("Unknown node type: " + nodeType);
		}


		public long Create() {
			if (initialized) {
				throw new InvalidOperationException("This tree store is already initialized.");
			}

			// Temporary node heap for creating a starting database
			TreeNodeHeap nodeHeap = new TreeNodeHeap(17, 4*1024*1024);

			// Write a root node to the store,
			// Create an empty head node
			TreeLeaf headLeaf = nodeHeap.CreateLeaf(null, Key.Head, 256);
			// Insert a tree identification pattern
			headLeaf.Write(0, new byte[] {1, 1, 1, 1}, 0, 4);
			// Create an empty tail node
			TreeLeaf tailLeaf = nodeHeap.CreateLeaf(null, Key.Tail, 256);
			// Insert a tree identification pattern
			tailLeaf.Write(0, new byte[] {1, 1, 1, 1}, 0, 4);

			// The write sequence,
			TreeWrite seq = new TreeWrite();
			seq.NodeWrite(headLeaf);
			seq.NodeWrite(tailLeaf);
			IList<NodeId> refs = Persist(seq);

			// Create a branch,
			TreeBranch rootBranch = nodeHeap.CreateBranch(null, MaxBranchSize);
			rootBranch.Set(refs[0], 4, Key.Tail, refs[1], 4);

			seq = new TreeWrite();
			seq.NodeWrite(rootBranch);
			refs = Persist(seq);

			// The written root node reference,
			NodeId rootId = refs[0];

			// Delete the head and tail leaf, and the root branch
			nodeHeap.Delete(headLeaf.Id);
			nodeHeap.Delete(tailLeaf.Id);
			nodeHeap.Delete(rootBranch.Id);

			// Write this version info to the store,
			long versionId = WriteSingleVersionInfo(1, rootId, new List<NodeId>(0));

			// Make a first version
			VersionInfo versionInfo = new VersionInfo(1, rootId, versionId);
			versions.Add(versionInfo);

			// Flush this to the version list
			IAreaWriter versionList = nodeStore.CreateArea(64);
			versionList.WriteInt4(0x01433);
			versionList.WriteInt4(1);
			versionList.WriteInt8(versionId);
			versionList.Finish();

			// Get the versions id
			long versionListId = versionList.Id;

			// The final header
			IAreaWriter header = nodeStore.CreateArea(64);
			header.WriteInt4(0x09391); // The magic value,
			header.WriteInt4(1); // The version
			header.WriteInt8(versionListId);
			header.Finish();

			// Set up the internal variables,
			headerId = header.Id;

			initialized = true;
			// And return the header reference
			return headerId;
		}

		public void Init(long headerId) {
			if (initialized)
				throw new ApplicationException("This tree store is already initialized.");

			// Set the header id
			this.headerId = headerId;

			// Get the header area
			IArea headerArea = nodeStore.GetArea(headerId);
			headerArea.Position = 8;
			// Read the versions list,
			long versionListId = headerArea.ReadInt8();

			// Read the versions list area
			// magic(int), versions count(int), list of version id objects.
			IArea versionsArea = nodeStore.GetArea(versionListId);
			if (versionsArea.ReadInt4() != 0x01433)
				throw new IOException("Incorrect magic value 0x01433");

			int versCount = versionsArea.ReadInt4();
			// For each id from the versions area, read in the associated VersionInfo
			// object into the 'vers' array.
			VersionInfo[] vers = new VersionInfo[versCount];
			for (int i = 0; i < versCount; ++i) {
				vers[i] = ReadSingleVersionInfo(versionsArea.ReadInt8());
			}

			// Set up the versions object
			for (int i = 0; i < versCount; ++i) {
				versions.Add(vers[i]);
			}
			// If more than two uncomitted versions, dispose them
			if (versions.Count > 2) {
				DisposeOldVersions();
			}

			initialized = true;
		}

		public TreeSystemTransaction CreateTransaction() {
			CheckErrorState();
			try {
				// Returns the latest snapshot (the snapshot at the end of the versions
				// list)
				VersionInfo info;
				lock (versions) {
					info = versions[versions.Count - 1];
					info.Lock();
				}
				return CreateSnapshot(info);
			} catch (OutOfMemoryException e) {
				// A virtual machine error most often means the VM ran out of memory,
				// which represents a critical state that causes immediate cleanup.
				throw SetErrorState(e);
			}
		}

		public void Commit(ITransaction tran) {
			CheckErrorState();
			try {
				TreeSystemTransaction transaction = (TreeSystemTransaction) tran;
				VersionInfo topVersion;
				lock (versions) {
					topVersion = versions[versions.Count - 1];
				}
				// Check the version is based on the must current transaction,
				if (transaction.VersionId != topVersion.VersionId) {
					// ID not the same as the top version, so throw the exception
					throw new ApplicationException("Can't commit non-sequential version.");
				}

				// Make sure the transaction is written to the store,
				// NOTE: This MUST happen outside a node store lock otherwise checking
				//   out on the cache manage function could lock up the thread
				transaction.Checkout();

				try {
					nodeStore.LockForWrite();

					// The new version number,
					long newVersionNum = topVersion.VersionId + 1;

					// Write out the versions list to the store,
					long versionRecordId = WriteVersionsList(newVersionNum, transaction);
					// Create a new VersionInfo object with a new id,
					VersionInfo newVinfo = new VersionInfo(newVersionNum,
					                                        transaction.RootNodeId,
					                                        versionRecordId);
					lock (versions) {
						// Add this version to the end of the versions list,
						versions.Add(newVinfo);
					}

				} finally {
					nodeStore.UnlockForWrite();
				}

				// Notify the transaction is committed,
				// This will stop the transaction from cleaning up newly added nodes.
				transaction.OnCommitted();
			} catch (IOException e) {
				// An IOException during this block represents a critical stopping
				// condition.
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				// An out-of-memory error represents a critical state that causes 
				// immediate cleanup.
				throw SetErrorState(e);
			}
		}

		public void Dispose(ITransaction tran) {
			CheckErrorState();
			try {
				TreeSystemTransaction transaction = (TreeSystemTransaction)tran;
				// Get the version id of the transaction,
				long versionId = transaction.VersionId;
				// Call the dispose method,
				transaction.Dispose();
				// Reduce the lock count for this version id,
				UnlockTransaction(versionId);
				// Check if we can clear up old versions,
				DisposeOldVersions();
			} catch (IOException e) {
				// An IOException during this block represents a critical stopping
				// condition.
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				// An out-of-memory error represents a critical state that causes 
				// immediate cleanup.
				throw SetErrorState(e);
			}
		}


		public void CheckPoint() {
			CheckErrorState();
			try {
				try {
					nodeStore.CheckPoint();
				} catch (ThreadInterruptedException) {
					// Ignore interrupted exceptions
				} 
			} catch (IOException e) {
				// An IOException during this block represents a critical stopping
				// condition.
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				// An out-of-memory error represents a critical state that causes 
				// immediate cleanup.
				throw SetErrorState(e);
			}
		}

		public TreeReportNode CreateDiagnosticGraph() {
			CheckErrorState();

			// Create the header node
			TreeReportNode header_node = new TreeReportNode("header", headerId);

			// Get the header area
			IArea header_area = nodeStore.GetArea(headerId);
			header_area.Position = 8;
			// Read the versions list,
			long version_list_ref = header_area.ReadInt8();

			// Create the version node
			TreeReportNode versions_node =
				new TreeReportNode("versions list", version_list_ref);
			// Set this as a child to the header
			header_node.ChildNodes.Add(versions_node);

			// Read the versions list area
			// magic(int), versions count(int), list of version id objects.
			IArea versions_area = nodeStore.GetArea(version_list_ref);
			if (versions_area.ReadInt4() != 0x01433)
				throw new IOException("Incorrect magic value 0x01433");

			int versCount = versions_area.ReadInt4();
			// For each id from the versions area, read in the associated VersionInfo
			// object into the 'vers' array.
			for (int i = 0; i < versCount; ++i) {
				long vInfoRef = versions_area.ReadInt8();
				// Set up the information in our node
				TreeReportNode vInfoNode = new TreeReportNode("version", vInfoRef);

				// Read in the version information node
				IArea vInfoArea = nodeStore.GetArea(vInfoRef);
				int magic = vInfoArea.ReadInt4();
				int ver = vInfoArea.ReadInt4();
				long versionId = vInfoArea.ReadInt8();
				long rnrHigh = vInfoArea.ReadInt8();
				long rnrLow = vInfoArea.ReadInt8();
				NodeId rootNodeId = new NodeId(rnrHigh, rnrLow);

				vInfoNode.SetProperty("MAGIC", magic);
				vInfoNode.SetProperty("VER", ver);
				vInfoNode.SetProperty("version_id", versionId);
				// Make the deleted area list into a property
				int deletedAreaCount = vInfoArea.ReadInt4();
				if (deletedAreaCount > 0) {
					for (int n = 0; n < deletedAreaCount; ++n) {
						long delnHigh = vInfoArea.ReadInt8();
						long delnLow = vInfoArea.ReadInt8();
						NodeId delNodeId = new NodeId(delnHigh, delnLow);
						vInfoNode.ChildNodes.Add(new TreeReportNode("delete", delNodeId));
					}
				}

				// Add the child node (the root node of the version graph).
				vInfoNode.ChildNodes.Add(CreateDiagnosticRootGraph(Key.Head, rootNodeId));

				// Add this to the version list node
				versions_node.ChildNodes.Add(vInfoNode);
			}

			// Return the header node
			return header_node;
		}

		private TreeReportNode CreateDiagnosticRootGraph(Key leftKey, NodeId id) {
			// The node being returned
			TreeReportNode node;

			// Open the area
			IArea area = nodeStore.GetArea(ToInt64StoreAddress(id));
			// What type of node is this?
			short nodeType = area.ReadInt2();
			// The version
			short ver = area.ReadInt2();
			if (nodeType == StoreLeafType) {
				// Read the reference count,
				long refCount = area.ReadInt4();
				// The number of bytes in the leaf
				int leafSize = area.ReadInt4();

				// Set up the leaf node object
				node = new TreeReportNode("leaf", id);
				node.SetProperty("VER", ver);
				node.SetProperty("key", leftKey.ToString());
				node.SetProperty("reference_count", refCount);
				node.SetProperty("leaf_size", leafSize);

			} else if (nodeType == StoreBranchType) {
				// The data size area containing the children information
				int childDataSize = area.ReadInt4();
				long[] dataArr = new long[childDataSize];
				for (int i = 0; i < childDataSize; ++i) {
					dataArr[i] = area.ReadInt8();
				}
				// Create the TreeBranch object to query it
				TreeBranch branch = new TreeBranch(id, dataArr, childDataSize);
				// Set up the branch node object
				node = new TreeReportNode("branch", id);
				node.SetProperty("VER", ver);
				node.SetProperty("key", leftKey.ToString());
				node.SetProperty("branch_size", branch.ChildCount);
				// Recursively add each child into the tree
				for (int i = 0; i < branch.ChildCount; ++i) {
					NodeId childId = branch.GetChild(i);
					// If the id is a special node, skip it
					if (childId.IsSpecial) {
						// Should we record special nodes?
					} else {
						Key newLeftKey = (i > 0) ? branch.GetKey(i) : leftKey;
						TreeReportNode bn = new TreeReportNode("child_meta", id);
						bn.SetProperty("extent", branch.GetChildLeafElementCount(i));
						node.ChildNodes.Add(bn);
						node.ChildNodes.Add(CreateDiagnosticRootGraph(newLeftKey, childId));
					}
				}

			} else {
				throw new IOException("Unknown node type: " + nodeType);
			}

			return node;
		}


		public IList<ITreeNode> FetchNodes(NodeId[] nids) {
			int sz = nids.Length;
			ITreeNode[] nodeResults = new ITreeNode[sz];
			for (int i = 0; i < sz; ++i) {
				nodeResults[i] = FetchNode(nids[i]);
			}
			return nodeResults;
		}

		public bool IsNodeAvailable(NodeId nodeId) {
			// Special node ref,
			if (nodeId.IsSpecial)
				return true;

			// Otherwise return true (all data for store backed tree systems is local),
			return true;
		}

		public bool LinkLeaf(Key key, NodeId id) {
			// If the node is a special node, then we don't need to reference count it.
			if (id.IsSpecial) {
				return true;
			}
			try {
				nodeStore.LockForWrite();

				// Get the area as a MutableArea object
				IMutableArea leafArea = nodeStore.GetMutableArea(ToInt64StoreAddress(id));
				// We synchronize over a reference count lock
				// (Pending: should we lock by area instead?  I'm not sure it will be
				//  worth the complexity of a more fine grained locking mechanism for the
				//  performance improvements - maybe we should implement a locking
				//  mechanism inside MutableArea).
				lock (referenceCountLock) {
					// Assert this is a leaf
						leafArea.Position = 0;
						short node_type = leafArea.ReadInt2();
					if (node_type != StoreLeafType)
						throw new IOException("Can only link to a leaf node.");

					leafArea.Position = 4;
					int refCount = leafArea.ReadInt4();
					// If reference counter is near overflowing, return false.
					if (refCount > Int32.MaxValue - 8)
						return false;

					leafArea.Position = 4;
					leafArea.WriteInt4(refCount + 1);
				}
				return true;
			} finally {
				nodeStore.UnlockForWrite();
			}
		}

		public void DisposeNode(NodeId nid) {
			try {
				nodeStore.LockForWrite();

				DoDisposeNode(nid);
			} finally {
				nodeStore.UnlockForWrite();
			}
		}

		public ErrorStateException SetErrorState(Exception error) {
			critical_stop_error = new ErrorStateException(error.Message, error);
			return critical_stop_error;
		}

		public void CheckErrorState() {
			if (critical_stop_error != null) {
				// We wrap the critical stop error a second time to ensure the stack
				// trace accurately reflects where the failure originated.
				throw new ErrorStateException(critical_stop_error.Message, critical_stop_error);
			}
		}

		public IList<NodeId> Persist(TreeWrite write) {
			try {
				nodeStore.LockForWrite();

				IList<ITreeNode> allBranches = write.BranchNodes;
				IList<ITreeNode> allLeafs = write.LeafNodes;
				List<ITreeNode> nodes = new List<ITreeNode>(allBranches.Count + allLeafs.Count);
				nodes.AddRange(allBranches);
				nodes.AddRange(allLeafs);

				// The list of nodes to be allocated,
				int sz = nodes.Count;
				// The list of allocated referenced for the nodes,
				NodeId[] refs = new NodeId[sz];
				// The list of area writers,
				IAreaWriter[] writers = new IAreaWriter[sz];

				// Allocate the space first,
				for (int i = 0; i < sz; ++i) {
					ITreeNode node = nodes[i];
					// Is it a branch node?
					if (node is TreeBranch) {
						TreeBranch branch = (TreeBranch) node;
						int ndsz = branch.NodeDataSize;
						writers[i] = nodeStore.CreateArea(4 + 4 + (ndsz*8));
					}
						// Otherwise, it must be a leaf node,
					else {
						TreeLeaf leaf = (TreeLeaf) node;
						int lfsz = leaf.Length;
						writers[i] = nodeStore.CreateArea(12 + lfsz);
					}
					// Set the reference,
					refs[i] = FromInt64StoreAddress(writers[i].Id);
				}

				// Now write out the data,
				for (int i = 0; i < sz; ++i) {
					ITreeNode node = nodes[i];
					// Is it a branch node?
					if (node is TreeBranch) {
						TreeBranch branch = (TreeBranch) node;

						// The number of children
						int chsz = branch.ChildCount;
						// For each child, if it's a heap node, look up the child id and
						// reference map in the sequence and set the reference accordingly,
						for (int o = 0; o < chsz; ++o) {
							NodeId childId = branch.GetChild(o);
							if (childId.IsInMemory) {
								// The ref is currently on the heap, so adjust accordingly
								int refId = write.LookupRef(i, o);
								branch.SetChild(refs[refId], o);
							}
						}

						// Write out the branch to the store
						long[] nodeData = branch.NodeData;
						int ndsz = branch.NodeDataSize;

						IAreaWriter writer = writers[i];
						writer.WriteInt2(StoreBranchType);
						writer.WriteInt2(1); // version
						writer.WriteInt4(ndsz);
						for (int o = 0; o < ndsz; ++o) {
							writer.WriteInt8(nodeData[o]);
						}
						writer.Finish();

						// Make this into a branch node and add to the cache,
						branch = new TreeBranch(refs[i], nodeData, ndsz);
						// Put this branch in the cache,
						lock (branchCache) {
							branchCache.Set(refs[i], branch);
						}

					}
						// Otherwise, it must be a leaf node,
					else {
						TreeLeaf leaf = (TreeLeaf) node;
						IAreaWriter writer = writers[i];
						writer.WriteInt2(StoreLeafType);
						writer.WriteInt2(1); // version
						writer.WriteInt4(1); // reference count
						writer.WriteInt4(leaf.Length);
						leaf.WriteTo(writer);
						writer.Finish();
					}
				}

				return refs;
			} finally {
				nodeStore.UnlockForWrite();
			}
		}

		#region VersionInfo

		private class VersionInfo {
			private readonly long versionId;
			private readonly NodeId rootNodePointer;
			private readonly long versionInfoRef;

			private int lockCount;


			public VersionInfo(long versionId, NodeId rootNodePointer, long versionInfoRef) {
				this.versionId = versionId;
				this.rootNodePointer = rootNodePointer;
				this.versionInfoRef = versionInfoRef;
			}

			public long VersionId {
				get { return versionId; }
			}

			public NodeId RootNodeId {
				get { return rootNodePointer; }
			}

			public bool NotLocked {
				get { return lockCount == 0; }
			}

			public long VersionInfoRef {
				get { return versionInfoRef; }
			}

			public void Lock() {
				++lockCount;
			}

			public void Unlock() {
				--lockCount;
				if (lockCount < 0) {
					throw new ApplicationException("Lock error.");
				}
			}

			public override bool Equals(object ob) {
				VersionInfo destV = (VersionInfo) ob;
				return (destV.versionId == versionId &&
				        destV.rootNodePointer.Equals(rootNodePointer));
			}

			public override int GetHashCode() {
				return versionId.GetHashCode() +
				       rootNodePointer.GetHashCode();
			}
		}


		#endregion

		#region AreaTreeLeaf

		private class AreaTreeLeaf : TreeLeaf {
			private readonly IArea area;
			private readonly int leafSize;
			private readonly NodeId nodeId;


			public AreaTreeLeaf(NodeId nodeId, int leafSize, IArea area) {
				this.nodeId = nodeId;
				this.leafSize = leafSize;
				this.area = area;
			}

			public override int Length {
				get { return leafSize; }
			}

			public override int Capacity {
				get { throw new ApplicationException("Area leaf does not have a meaningful capacity."); }
			}

			public override NodeId Id {
				get { return nodeId; }
			}

			public override long MemoryAmount {
				get { throw new NotSupportedException(); }
			}

			public override void SetLength(int value) {
				throw new IOException("Write methods not available for immutable store leaf.");
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				area.Position = position + 12;  // Make sure we position past the headers
				area.Read(buffer, offset, count);
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				throw new IOException("Write methods not available for immutable store leaf.");
			}

			public override void WriteTo(IAreaWriter dest) {
				area.Position = 12;
				area.CopyTo(dest, Length);
			}

			public override void Shift(int position, int offset) {
				throw new IOException("Write methods not available for immutable store leaf.");
			}
		}

		#endregion
	}
}