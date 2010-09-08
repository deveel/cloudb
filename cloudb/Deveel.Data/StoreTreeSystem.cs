using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

using Deveel.Data.Caching;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="ITreeSystem"/> that is wrapped
	/// on a <see cref="IStore"/>.
	/// </summary>
	public sealed class StoreTreeSystem : ITreeSystem {
		private volatile ErrorStateException errorState = null;

		private readonly List<VersionInfo> versions;
		private readonly long nodeHeapMaxSize;
		private readonly IStore store;
		private readonly int maxBranchSize;
		private readonly int maxLeafByteSize;
		private readonly Cache branchCache;
		private bool initialized;

		private long headerId;

		private readonly object refCountLock = new object();

		// The type identifiers for branch and leaf nodes in the tree.
		private const short LeafType = 0x019EC;
		private const short BranchType = 0x022EB;

		public StoreTreeSystem(IStore store, int maxBranchSize,
									   int maxLeafByteSize, long nodeMaxCacheMemory,
									   long branchCacheMemory) {
			this.maxBranchSize = maxBranchSize;
			this.maxLeafByteSize = maxLeafByteSize;
			this.store = store;
			this.nodeHeapMaxSize = nodeMaxCacheMemory;
			this.versions = new List<VersionInfo>();

			// Allocate some values for the branch cache,
			long branchSizeEstimate = (maxBranchSize * 24) + 64;
			// The number of elements in the branch cache
			int branchCacheElements = (int)(branchCacheMemory / branchSizeEstimate);

			// Find a close prime to this
			int branch_prime = Cache.ClosestPrime(branchCacheElements + 20);
			// Allocate the cache
			this.branchCache = new MemoryCache(branch_prime, branchCacheElements, 20);

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

		private long WriteVersionInfo(long versionId, long rootNodeId, IList<long> deletedRefs) {
			int delRefCount = deletedRefs.Count;

			// Write the version info and the deleted refs to a new area,
			IAreaWriter area = store.CreateArea(4 + 4 + 8 + 8 + 4 + (delRefCount * 8));
			area.WriteInt4(0x04EA23);
			area.WriteInt4(1);
			area.WriteInt8(versionId);
			area.WriteInt8(rootNodeId);
			area.WriteInt4(delRefCount);
			for (int i = 0; i < delRefCount; ++i) {
				area.WriteInt8(deletedRefs[i]);
			}
			area.Finish();

			return area.Id;
		}

		private VersionInfo ReadVersionInfo(long versionRef) {
			IArea verArea = store.GetArea(versionRef);
			int magic = verArea.ReadInt4();
			int version = verArea.ReadInt4();
			long versionId = verArea.ReadInt8();
			long rootNodeId = verArea.ReadInt8();

			if (magic != 0x04EA23)
				throw new IOException("Incorrect magic value.");
			if (version < 1)
				throw new IOException("Version incorrect.");

			return new VersionInfo(versionId, rootNodeId, versionRef);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		private long WriteVersionsList(long versionId, TreeSystemTransaction tran) {
			// Write the version info and the deleted refs to a new area,
			long rootNodeId = tran.RootNodeId;
			if (rootNodeId < 0)
				throw new ApplicationException("Assertion failed, root_node is on heap.");

			// Get the list of all nodes deleted in the transaction
			IList<long> deletedNodes = tran.DeletedNodes;

			// Sort it
			((List<long>)deletedNodes).Sort();

			// Check for any duplicate entries (we shouldn't double delete stuff).
			for (int i = 1; i < deletedNodes.Count; ++i) {
				if (deletedNodes[i - 1] == deletedNodes[i]) {
					// Oops, duplicated delete
					throw new ApplicationException("Duplicate records in delete list.");
				}
			}

			long verId = WriteVersionInfo(versionId, rootNodeId, deletedNodes);

			// Now update the version list by copying the list and adding the new ref
			// to the end.

			// Get the current version list
			IMutableArea headerArea = store.GetMutableArea(headerId);
			headerArea.Position = 8;
			long versionListId = headerArea.ReadInt8();

			// Read information from the old version info,
			IArea verListArea = store.GetArea(versionListId);
			verListArea.ReadInt4();  // The magic
			int versionCount = verListArea.ReadInt4();

			// Create a new list,
			IAreaWriter newVerListArea = store.CreateArea(8 + (8 * (versionCount + 1)));
			newVerListArea.WriteInt4(0x01433);
			newVerListArea.WriteInt4(versionCount + 1);
			for (int i = 0; i < versionCount; ++i) {
				newVerListArea.WriteInt8(verListArea.ReadInt8());
			}
			newVerListArea.WriteInt8(verId);
			newVerListArea.Finish();

			// Write the new area to the header,
			headerArea.Position = 8;
			headerArea.WriteInt8(newVerListArea.Id);

			// Delete the old version list Area,
			store.DeleteArea(versionListId);

			// Done,
			return verId;
		}

		private ITreeNode FetchNode(long nodeId) {
			// Is it a special static node?
			if ((nodeId & 0x01000000000000000L) != 0)
				return SparseLeafNode.Create(nodeId);

			// Is this a branch node in the cache?
			TreeBranch branch;
			lock (branchCache) {
				branch = (TreeBranch)branchCache.Get(nodeId);
				if (branch != null)
					return branch;
			}

			// Not found in the cache, so fetch the area from the backing store and
			// create the node type.

			// Get the area for the node
			IArea nodeArea = store.GetArea(nodeId);
			// Wrap around a BinaryReader for reading values from the store.
			BinaryReader reader = new BinaryReader(new AreaInputStream(nodeArea, 256));

			short nodeType = reader.ReadInt16();

			if (nodeType == LeafType) {
				// Read the key
				reader.ReadInt16();  // version
				reader.ReadInt32();   // reference count
				int leafSize = reader.ReadInt32();

				// Return a leaf that's mapped to the data in the store
				nodeArea.Position = 0;
				return new AreaTreeLeaf(nodeId, leafSize, nodeArea);
			} else if (nodeType == BranchType) {
				// Note that the entire branch is loaded into memory now,
				reader.ReadInt16();  // version
				int childDataSize = reader.ReadInt32();
				long[] data = new long[childDataSize];
				for (int i = 0; i < childDataSize; ++i) {
					data[i] = reader.ReadInt64();
				}

				branch = new TreeBranch(nodeId, data, childDataSize);
				// Put this branch in the cache,
				lock (branchCache) {
					branchCache.Set(nodeId, branch);
					// And return the branch
					return branch;
				}
			}

			throw new ApplicationException("Unknown node type: " + nodeType);
		}

		private void UnlockVersion(long versionId) {
			lock (versions) {
				int sz = versions.Count;
				for (int i = sz - 1; i >= 0; --i) {
					VersionInfo vinfo = versions[i];
					if (vinfo.VersionId == versionId) {
						// Unlock this version,
						vinfo.Unlock();
						return;
					}
				}
			}

			throw new ApplicationException("Unable to find version to unlock: " + versionId);
		}

		private void DoDisposeNode(long nodeId) {
			// If the node is a special node, then we don't dispose it
			if ((nodeId & 0x01000000000000000L) != 0)
				return;

			// Is it a leaf node?
			IMutableArea nodeArea = store.GetMutableArea(nodeId);
			nodeArea.Position = 0;
			int nodeType = nodeArea.ReadInt2();
			if (nodeType == LeafType) {
				// Yes, get its reference_count,
				lock (refCountLock) {
					nodeArea.Position = 4;
					int ref_count = nodeArea.ReadInt4();
					// If the reference_count is >1 then decrement it and return
					if (ref_count > 1) {
						nodeArea.Position = 4;
						nodeArea.WriteInt4(ref_count - 1);
						return;
					}
				}
			} else if (nodeType != BranchType) {
				// Has to be a branch type, otherwise failure
				throw new IOException("Unknown node type.");
			}

			// it is a none leaf branch or its reference count is 1, so delete the
			// area.

			// NOTE, we delete from the cache first before we delete the area
			//   because the deleted area may be reclaimed immediately and deleting
			//   from the cache after may be too late.

			// Delete from the cache because the given ref may be recycled for a new
			// node at some point.
			lock (branchCache) {
				branchCache.Remove(nodeId);
			}

			// Delete the area
			store.DeleteArea(nodeId);
		}

		private void DisposeOldVersions() {
			List<VersionInfo> disposeList = new List<VersionInfo>();
			lock (versions) {
				// size - 1 because we don't want to delete the very last version,
				int sz = versions.Count - 1;
				bool found_locked_entry = false;
				for (int i = 0; i < sz && found_locked_entry == false; ++i) {
					VersionInfo vinfo = versions[i];
					// If this version isn't locked,
					if (vinfo.IsNotLocked) {
						// Add to the dispose list
						disposeList.Add(vinfo);
						// And delete from the versions list,
						versions.RemoveAt(i);
						--sz;
						--i;
					} else {
						// If it is locked, we exit the loop
						found_locked_entry = true;
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
						store.LockForWrite();

						// First we write out a modified version header minus the versions we
						// are to delete,

						// Get the current version list
						IMutableArea headerArea = store.GetMutableArea(headerId);
						headerArea.Position = 8;
						long versionListId = headerArea.ReadInt8();

						// Read information from the old version info,
						IArea verListArea = store.GetArea(versionListId);
						verListArea.ReadInt4();  // The magic
						int versionCount = verListArea.ReadInt4();

						int newVersionCount = versionCount - disposeList.Count;
						// Create a new list,
						IAreaWriter newVersionList = store.CreateArea(8 + (8 * newVersionCount));
						newVersionList.WriteInt4(0x01433);
						newVersionList.WriteInt4(newVersionCount);
						// Skip the versions we are deleting,
						for (int i = 0; i < disposeList.Count; ++i) {
							verListArea.ReadInt8();
						}
						// Now copy the list from the new point
						for (int i = 0; i < newVersionCount; ++i) {
							newVersionList.WriteInt8(verListArea.ReadInt8());
						}
						newVersionList.Finish();

						// Write the new area to the header,
						headerArea.Position = 8;
						headerArea.WriteInt8(newVersionList.Id);

						// Delete the old version list Area,
						store.DeleteArea(versionListId);

						// Dispose the version info,
						int sz = disposeList.Count;
						for (int i = 0; i < sz; ++i) {
							VersionInfo vinfo = disposeList[i];
							long v_ref = vinfo.versionInfoRef;
							IArea version_area = store.GetArea(v_ref);
							int magic = version_area.ReadInt4();
							int rev = version_area.ReadInt4();
							// Check the magic,
							if (magic != 0x04EA23)
								throw new ApplicationException("Magic value for version area is incorrect.");

							version_area.ReadInt8();		// version id
							version_area.ReadInt8();		// root node id

							int nodeCount = version_area.ReadInt4();
							// For each node,
							for (int n = 0; n < nodeCount; ++n) {
								// Read the next area
								long delNodeRef = version_area.ReadInt8();
								// Cleanly disposes the node
								DoDisposeNode(delNodeRef);
							}

							// Delete the node header,
							store.DeleteArea(v_ref);
						}
					} finally {
						store.UnlockForWrite();
					}
				}
			}
		}

		private TreeSystemTransaction CreateSnapshot(VersionInfo vinfo) {
			return new TreeSystemTransaction(this, vinfo.VersionId, vinfo.RootNodeId, false);
		}

		private TreeGraph CreateRootGraph(Key leftKey, long reference) {
			// The node being returned
			TreeGraph graph;

			// Open the area
			IArea area = store.GetArea(reference);
			// What type of node is this?
			short nodeType = area.ReadInt2();
			// The version
			short ver = area.ReadInt2();
			if (nodeType == LeafType) {
				// Read the reference count,
				long refCount = area.ReadInt4();
				// The number of bytes in the leaf
				int leafSize = area.ReadInt4();

				// Set up the leaf node object
				graph = new TreeGraph("leaf", reference);
				graph.SetProperty("ver", ver);
				graph.SetProperty("key", leftKey.ToString());
				graph.SetProperty("reference_count", refCount);
				graph.SetProperty("leaf_size", leafSize);

			} else if (nodeType == BranchType) {
				// The data size area containing the children information
				int childDataSize = area.ReadInt4();
				long[] data = new long[childDataSize];
				for (int i = 0; i < childDataSize; ++i) {
					data[i] = area.ReadInt8();
				}
				// Create the TreeBranch object to query it
				TreeBranch branch = new TreeBranch(reference, data, childDataSize);
				// Set up the branch node object
				graph = new TreeGraph("branch", reference);
				graph.SetProperty("ver", ver);
				graph.SetProperty("key", leftKey.ToString());
				graph.SetProperty("branch_size", branch.ChildCount);
				// Recursively add each child into the tree
				for (int i = 0; i < branch.ChildCount; ++i) {
					long child_ref = branch.GetChild(i);
					// If the ref is a special node, skip it
					if ((child_ref & 0x01000000000000000L) != 0) {
						// Should we record special nodes?
					} else {
						Key newLeftKey = (i > 0) ? branch.GetKey(i) : leftKey;
						TreeGraph bn = new TreeGraph("child_meta", reference);
						bn.SetProperty("extent", branch.GetChildLeafElementCount(i));
						graph.AddChild(bn);
						graph.AddChild(CreateRootGraph(newLeftKey, child_ref));
					}
				}
			} else {
				throw new IOException("Unknown node type: " + nodeType);
			}

			return graph;
		}

		public long Create() {
			if (initialized)
				throw new ApplicationException("This tree store is already initialized.");

			// Temporary node heap for creating a starting database
			TreeNodeHeap nodeHeap = new TreeNodeHeap(17, 4 * 1024 * 1024);

			// Write a root node to the store,
			// Create an empty head node
			TreeLeaf headLeaf = nodeHeap.CreateLeaf(null, Key.Head, 256);
			// Insert a tree identification pattern
			headLeaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
			// Create an empty tail node
			TreeLeaf tailLeaf = nodeHeap.CreateLeaf(null, Key.Tail, 256);
			// Insert a tree identification pattern
			tailLeaf.Write(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

			// The write sequence,
			TreeWrite treeWrite = new TreeWrite();
			treeWrite.NodeWrite(headLeaf);
			treeWrite.NodeWrite(tailLeaf);
			IList<long> refs = Persist(treeWrite);

			// Create a branch,
			TreeBranch rootBranch = nodeHeap.CreateBranch(null, MaxBranchSize);
			rootBranch.Set(refs[0], 4,
						   Key.Tail.GetEncoded(1),
						   Key.Tail.GetEncoded(2),
						   refs[1], 4);

			treeWrite = new TreeWrite();
			treeWrite.NodeWrite(rootBranch);
			refs = Persist(treeWrite);

			// The written root node reference,
			long rootId = refs[0];

			// Delete the head and tail leaf, and the root branch
			nodeHeap.Delete(headLeaf.Id);
			nodeHeap.Delete(tailLeaf.Id);
			nodeHeap.Delete(rootBranch.Id);

			// Write this version info to the store,
			long versionId = WriteVersionInfo(1, rootId, new List<long>(0));

			// Make a first version
			versions.Add(new VersionInfo(1, rootId, versionId));

			// Flush this to the version list
			IAreaWriter verListArea = store.CreateArea(64);
			verListArea.WriteInt4(0x01433);
			verListArea.WriteInt4(1);
			verListArea.WriteInt8(versionId);
			verListArea.Finish();
			// Get the versions id
			long versionListId = verListArea.Id;

			// The final header
			IAreaWriter header = store.CreateArea(64);
			header.WriteInt4(0x09391);   // The magic value,
			header.WriteInt4(1);         // The version
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
			IArea headerArea = store.GetArea(this.headerId);
			headerArea.Position = 8;
			// Read the versions list,
			long verListId = headerArea.ReadInt8();

			// Read the versions list area
			// magic(int), versions count(int), list of version id objects.
			IArea verArea = store.GetArea(verListId);
			if (verArea.ReadInt4() != 0x01433)
				throw new IOException("Incorrect magic value 0x01433");

			int versCount = verArea.ReadInt4();
			// For each id from the versions area, read in the associated VersionInfo
			// object into the 'vers' array.
			VersionInfo[] vers = new VersionInfo[versCount];
			for (int i = 0; i < versCount; ++i) {
				vers[i] = ReadVersionInfo(verArea.ReadInt8());
			}

			// Set up the versions object
			for (int i = 0; i < versCount; ++i) {
				versions.Add(vers[i]);
			}
			// If more than two uncomitted versions, dispose them
			if (versions.Count > 2)
				DisposeOldVersions();

			initialized = true;
		}

		public void CheckPoint() {
			CheckErrorState();

			try {
				try {
					store.CheckPoint();
				} catch (ThreadInterruptedException) {
				}
			} catch (IOException e) {
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				throw SetErrorState(e);
			}
		}

		public IList<ITreeNode> FetchNodes(long[] nids) {
			int sz = nids.Length;
			List<ITreeNode> nodes = new List<ITreeNode>(sz);
			for (int i = 0; i < sz; ++i) {
				nodes.Add(FetchNode(nids[i]));
			}
			return nodes;
		}

		public bool IsNodeAvailable(long node_ref) {
			// Special node ref,
			if ((node_ref & 0x01000000000000000L) != 0)
				return true;
			// Otherwise return true (all data for store backed tree systems is local),
			return true;
		}

		public bool LinkLeaf(Key key, long reference) {
			// If the node is a special node, then we don't need to reference count it.
			if ((reference & 0x01000000000000000L) != 0)
				return true;

			try {
				store.LockForWrite();

				// Get the area as a MutableArea object
				IMutableArea leafArea = store.GetMutableArea(reference);

				// We synchronize over a reference count lock
				// (Pending: should we lock by area instead?  Not sure it will be
				//  worth the complexity of a more fine grained locking mechanism for the
				//  performance improvements - maybe we should implement a locking
				//  mechanism inside IMutableArea).
				lock (refCountLock) {
					// Assert this is a leaf
					leafArea.Position = 0;
					short nodeType = leafArea.ReadInt2();
					if (nodeType != LeafType)
						throw new IOException("Can only link to a leaf node.");

					leafArea.Position = 4;
					int refCount = leafArea.ReadInt4();
					// If reference counter is near overflowing, return false,
					if (refCount > Int32.MaxValue - 8)
						return false;

					leafArea.Position = 4;
					leafArea.WriteInt4(refCount + 1);
				}

				return true;
			} finally {
				store.UnlockForWrite();
			}
		}

		public void DisposeNode(long nodeId) {
			try {
				store.LockForWrite();
				DoDisposeNode(nodeId);
			} finally {
				store.UnlockForWrite();
			}
		}

		public ErrorStateException SetErrorState(Exception error) {
			errorState = new ErrorStateException(error);
			return errorState;
		}

		public void CheckErrorState() {
			if (errorState != null) {
				// We wrap the critical stop error a second time to ensure the stack
				// trace accurately reflects where the failure originated.
				throw new ErrorStateException(errorState);
			}
		}

		public ITransaction CreateTransaction() {
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
				throw SetErrorState(e);
			}
		}

		public void Commit(ITransaction transaction) {
			CheckErrorState();
			try {
				TreeSystemTransaction treeTransaction = (TreeSystemTransaction)transaction;
				VersionInfo topVersion;
				lock (versions) {
					topVersion = versions[versions.Count - 1];
				}
				// Check the version is based on the must current transaction,
				if (treeTransaction.VersionId != topVersion.VersionId) {
					// ID not the same as the top version, so throw the exception
					throw new ApplicationException("Can't commit non-sequential version.");
				}

				// Make sure the transaction is written to the store,
				// NOTE: This MUST happen outside a node store lock otherwise checking
				//   out on the cache manage function could lock up the thread
				treeTransaction.CheckOut();

				try {
					store.LockForWrite();

					// The new version number,
					long new_version_num = topVersion.VersionId + 1;

					// Write out the versions list to the store,
					long version_record_id = WriteVersionsList(new_version_num, treeTransaction);
					// Create a new VersionInfo object with a new id,
					VersionInfo new_vinfo = new VersionInfo(new_version_num, treeTransaction.RootNodeId, version_record_id);
					lock (versions) {
						// Add this version to the end of the versions list,
						versions.Add(new_vinfo);
					}

				} finally {
					store.UnlockForWrite();
				}

				// Notify the transaction is committed,
				// This will stop the transaction from cleaning up newly added nodes.
				treeTransaction.OnCommitted();
			} catch (IOException e) {
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				throw SetErrorState(e);
			}
		}

		public void Dispose(ITransaction transaction) {
			CheckErrorState();

			try {
				TreeSystemTransaction treeTransaction = (TreeSystemTransaction)transaction;
				// Get the version id of the transaction,
				long version_id = treeTransaction.VersionId;
				// Call the dispose method,
				treeTransaction.Dispose();
				// Reduce the lock count for this version id,
				UnlockVersion(version_id);
				// Check if we can clear up old versions,
				DisposeOldVersions();
			} catch (IOException e) {
				throw SetErrorState(e);
			} catch (OutOfMemoryException e) {
				throw SetErrorState(e);
			}
		}

		public IList<long> Persist(TreeWrite write) {
			try {
				store.LockForWrite();

				IList<ITreeNode> branches = write.BranchNodes;
				IList<ITreeNode> leafs = write.LeafNodes;
				List<ITreeNode> nodes = new List<ITreeNode>(branches.Count + leafs.Count);
				nodes.AddRange(branches);
				nodes.AddRange(leafs);

				// The list of nodes to be allocated,
				int sz = nodes.Count;
				// The list of allocated referenced for the nodes,
				long[] refs = new long[sz];
				// The list of area writers,
				IAreaWriter[] areas = new IAreaWriter[sz];

				// Allocate the space first,
				for (int i = 0; i < sz; ++i) {
					ITreeNode node = nodes[i];
					if (node is TreeBranch) {
						TreeBranch branch = (TreeBranch)node;
						int ndsz = branch.DataSize;
						areas[i] = store.CreateArea(4 + 4 + (ndsz * 8));
					} else {
						TreeLeaf leaf = (TreeLeaf)node;
						int lfsz = leaf.Length;
						areas[i] = store.CreateArea(12 + lfsz);
					}
					// Set the reference,
					refs[i] = areas[i].Id;
				}

				// Now write out the data,
				for (int i = 0; i < sz; ++i) {
					ITreeNode node = nodes[i];
					// Is it a branch node?
					if (node is TreeBranch) {
						TreeBranch branch = (TreeBranch)node;

						// The number of children
						int chsz = branch.ChildCount;
						// For each child, if it's a heap node, look up the child id and
						// reference map in the sequence and set the reference accordingly,
						for (int o = 0; o < chsz; ++o) {
							long childId = branch.GetChild(o);
							if (childId < 0) {
								// The ref is currently on the heap, so adjust accordingly
								int refId = write.LookupRef(i, o);
								branch.SetChild(o, refs[refId]);
							}
						}

						// Write out the branch to the store
						long[] nodeData = branch.ChildPointers;
						int ndsz = branch.DataSize;

						IAreaWriter writer = areas[i];
						writer.WriteInt2(BranchType);
						writer.WriteInt2(1);  // version
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
					} else {
						// Otherwise, it must be a leaf node,
						TreeLeaf leaf = (TreeLeaf)node;

						IAreaWriter area = areas[i];
						area.WriteInt2(LeafType);
						area.WriteInt2(1);  // version
						area.WriteInt4(1);            // reference count
						area.WriteInt4(leaf.Length);
						leaf.WriteTo(area);
						area.Finish();
					}
				}

				return refs;
			} finally {
				store.UnlockForWrite();
			}
		}

		public TreeGraph CreateGraph() {
			CheckErrorState();

			// Create the header node
			TreeGraph headerGraph = new TreeGraph("header", headerId);

			// Get the header area
			IArea headerArea = store.GetArea(headerId);
			headerArea.Position = 8;
			// Read the versions list,
			long versionListRef = headerArea.ReadInt8();

			// Create the version node
			TreeGraph versions_node = new TreeGraph("versions_list", versionListRef);
			// Set this as a child to the header
			headerGraph.AddChild(versions_node);

			// Read the versions list area
			// magic(int), versions count(int), list of version id objects.
			IArea versArea = store.GetArea(versionListRef);
			if (versArea.ReadInt4() != 0x01433)
				throw new IOException("Incorrect magic value 0x01433");

			int vers_count = versArea.ReadInt4();
			// For each id from the versions area, read in the associated VersionInfo
			// object into the 'vers' array.
			for (int i = 0; i < vers_count; ++i) {
				long vInfoRef = versArea.ReadInt8();
				// Set up the information in our node
				TreeGraph vInfoGraph = new TreeGraph("version", vInfoRef);

				// Read in the version information node
				IArea vInfoArea = store.GetArea(vInfoRef);
				int magic = vInfoArea.ReadInt4();
				int ver = vInfoArea.ReadInt4();
				long versionId = vInfoArea.ReadInt8();
				long rootNodeRef = vInfoArea.ReadInt8();
				vInfoGraph.SetProperty("magic", magic);
				vInfoGraph.SetProperty("ver", ver);
				vInfoGraph.SetProperty("version_id", versionId);
				// Make the deleted area list into a property
				int deleted_area_count = vInfoArea.ReadInt4();
				if (deleted_area_count > 0) {
					for (int n = 0; n < deleted_area_count; ++n) {
						long delNodeRef = vInfoArea.ReadInt8();
						vInfoGraph.AddChild(new TreeGraph("delete", delNodeRef));
					}
				}

				// Add the child node (the root node of the version graph).
				vInfoGraph.AddChild(CreateRootGraph(Key.Head, rootNodeRef));

				// Add this to the version list node
				versions_node.AddChild(vInfoGraph);
			}

			// Return the header node
			return headerGraph;

		}

		#region VersionInfo

		public sealed class VersionInfo {
			private readonly long versionId;
			private readonly long rootNodeId;
			internal readonly long versionInfoRef;
			private int lockCount;

			public VersionInfo(long versionId, long rootNodeId, long versionInfoRef) {
				this.versionId = versionId;
				this.rootNodeId = rootNodeId;
				this.versionInfoRef = versionInfoRef;
			}

			public long VersionId {
				get { return versionId; }
			}

			public long RootNodeId {
				get { return rootNodeId; }
			}

			public bool IsNotLocked {
				get { return lockCount == 0; }
			}

			public void Lock() {
				++lockCount;
			}

			public void Unlock() {
				--lockCount;
				if (lockCount < 0)
					throw new ApplicationException("Lock error.");
			}

			public override bool Equals(object ob) {
				VersionInfo dest_v = (VersionInfo)ob;
				return (dest_v.versionId == versionId &&
						dest_v.rootNodeId == rootNodeId);
			}

			public override int GetHashCode() {
				return base.GetHashCode();
			}
		}

		#endregion

		#region AreaTreeLeaf

		class AreaTreeLeaf : TreeLeaf {
			private readonly IArea area;
			private readonly int leaf_size;
			private readonly long node_ref;

			public AreaTreeLeaf(long nodeId, int leafSize, IArea area)
				: base() {
				this.node_ref = nodeId;
				this.leaf_size = leafSize;
				this.area = area;
			}

			public override long Id {
				get { return node_ref; }
			}

			public override int Length {
				get { return leaf_size; }
			}

			public override int Capacity {
				get { throw new InvalidOperationException(); }
			}

			public override long MemoryAmount {
				get { throw new NotSupportedException(); }
			}

			public override void Read(int position, byte[] buffer, int offset, int count) {
				area.Position = position + 12;  // Make sure we position past the headers
				area.Read(buffer, offset, count);
			}

			public override void Write(int position, byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override void WriteTo(IAreaWriter destArea) {
				area.Position = 12;
				area.CopyTo(destArea, Length);
			}

			public override void Shift(int position, int offset) {
				throw new NotSupportedException();
			}

			public override void SetLength(int value) {
				throw new NotSupportedException();
			}
		}

		#endregion
	}
}