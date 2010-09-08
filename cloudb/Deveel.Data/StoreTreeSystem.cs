using System;
using System.Collections.Generic;

using Deveel.Data.Caching;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="ITreeSystem"/> that is wrapped
	/// on a <see cref="IStore"/>.
	/// </summary>
	public sealed class StoreTreeSystem : ITreeSystem {
		private volatile ErrorStateException critical_stop_error = null;
		
		private readonly List<VersionInfo> versions;
		private readonly long node_heap_max_size;
		private IStore node_store;
		private int max_branch_size;
		private int max_leaf_byte_size;
		private readonly Cache branch_cache;
		private bool initialized;

		private long header_id;
		
		private readonly object refCountLock = new object();
  
		// The type identifiers for branch and leaf nodes in the tree.
		private const short LeafType = 0x019EC;
		private const short BranchType = 0x022EB;
  
		public int MaxBranchSize {
			get {
				throw new NotImplementedException();
			}
		}
		
		public int MaxLeafByteSize {
			get {
				throw new NotImplementedException();
			}
		}
		
		public long NodeHeapMaxSize {
			get {
				throw new NotImplementedException();
			}
		}
		
		public void CheckPoint()
		{
			throw new NotImplementedException();
		}
		
		public IList<ITreeNode> FetchNodes(long[] nids)
		{
			throw new NotImplementedException();
		}
		
		public bool IsNodeAvailable(long node_ref)
		{
			throw new NotImplementedException();
		}
		
		public bool LinkLeaf(Key key, long reference)
		{
			throw new NotImplementedException();
		}
		
		public void DisposeNode(long nid)
		{
			throw new NotImplementedException();
		}
		
		public ErrorStateException SetErrorState(Exception error)
		{
			throw new NotImplementedException();
		}
		
		public void CheckErrorState()
		{
			throw new NotImplementedException();
		}
		
		public IList<long> Persist(TreeWrite write)
		{
			throw new NotImplementedException();
		}
		
		#region VersionInfo
		
		public sealed class VersionInfo {
			private long versionId;
			private long rootNodeId;
			internal long versionInfoRef;
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
				VersionInfo dest_v = (VersionInfo) ob;
				return (dest_v.versionId == versionId &&
				        dest_v.rootNodeId == rootNodeId);
			}
			
			public override int GetHashCode() {
				return base.GetHashCode();
			}
		}
		
		#endregion
	}
}