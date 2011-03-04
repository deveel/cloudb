using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Net.Client;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public abstract class RootService : Service {
		protected RootService(IServiceConnector connector, IServiceAddress address) {
			this.connector = connector;
			this.address = address;
			pathCache = new Dictionary<string, PathInfo>(128);
			lockDb = new Dictionary<string, PathAccess>(128);
			loadPathInfoQueue = new List<PathInfo>(128);

			serviceTracker = new ServiceStatusTracker(connector);
			serviceTracker.StatusChange += new ServiceStatusEventHandler(serviceTracker_StatusChange);
		}

		private readonly IServiceAddress address;
		private IServiceAddress[] managerAddresses;
		private readonly IServiceConnector connector;
		private readonly ServiceStatusTracker serviceTracker;
		private readonly Dictionary<string, PathInfo> pathCache;
		private readonly List<PathInfo> loadPathInfoQueue;
		private readonly Dictionary<string, PathAccess> lockDb;
		private readonly object loadPathInfoLock = new object();

		private const int RootItemSize = 24;
		
		public override ServiceType ServiceType {
			get { return ServiceType.Root; }
		}

		protected IServiceAddress[] ManagerAddresses {
			get {
				lock (this) {
					return managerAddresses;
				}
			}
			set {
				lock (this) {
					managerAddresses = (IServiceAddress[]) value.Clone();
				}
			}
		}
		
		private void serviceTracker_StatusChange(object sender, ServiceStatusEventArgs args) {
			// If it's a manager service, and the new status is UP
			if (args.ServiceType == ServiceType.Manager &&
			    args.NewStatus == ServiceStatus.Up) {
				// Init and load all pending paths,
				ProcessInitQueue();
				LoadPendingPaths();
			}
			// If it's a root service, and the new status is UP
			if (args.ServiceType == ServiceType.Root) {
				if (args.NewStatus == ServiceStatus.Up) {
					// Load all pending paths,
					LoadPendingPaths();
				}
					// If a root server went down,
				else if (args.NewStatus != ServiceStatus.Up) {
					// Scan the paths managed by this root server and desynchronize
					// any not connected to a majority of servers.
					DesyncPathsDependentOn(address);
				}
			}
		}

		private void ProcessInitQueue() {
			lock (loadPathInfoQueue) {
				OnInitQueue();
			}
		}

		protected virtual void OnInitQueue() {
			
		}

		protected void EnqueueLoadedPathInfo(PathInfo pathInfo) {
			lock (loadPathInfoQueue) {
				loadPathInfoQueue.Add(pathInfo);
			}
		}

		private void LoadPendingPaths() {
			// Make a copy of the pending path info loads,
			List<PathInfo> pi_list = new List<PathInfo>(64);
			lock (loadPathInfoQueue) {
				foreach (PathInfo pi in loadPathInfoQueue) {
					pi_list.Add(pi);
				}
			}
			// Do the load operation on the pending,
			try {
				foreach (PathInfo pi in pi_list) {
					LoadPathInfo(pi);
				}
			} catch (IOException e) {
				Logger.Error("IO Error", e);
			}
		}

		private void PollRootMachines(IServiceAddress[] machines) {
			// Create the message,
			RequestMessage request = new RequestMessage("poll");
			request.Arguments.Add("RSPoll");

			for (int i = 0; i < machines.Length; ++i) {
				IServiceAddress machine = machines[i];
				// If the service is up in the tracker,
				if (serviceTracker.IsServiceUp(machine, ServiceType.Root)) {
					// Poll it to see if it's really up.

					// Send the poll to the service,
					IMessageProcessor processor = connector.Connect(machine, ServiceType.Root);
					Message response = processor.Process(request);
					// If return is a connection fault,
					if (response.HasError && ReplicatedValueStore.IsConnectionFault(response))
						serviceTracker.ReportServiceDownClientReport(machine, ServiceType.Root);
				}
			}
		}

		private void DesyncPathsDependentOn(IServiceAddress root_service) {

			List<PathInfo> desynced_paths = new List<PathInfo>(64);

			lock (lockDb) {
				// The set of all paths,
				foreach (KeyValuePair<string, PathAccess> pair in lockDb) {
					// Get the PathInfo for the path,
					PathAccess path_access = pair.Value;
					// We have to be available to continue,
					if (path_access.IsSynchronized) {
						PathInfo path_info = path_access.PathInfo;
						// If it's null, we haven't finished initialization yet,
						if (path_info != null) {
							// Check if the path info is dependent on the service that changed
							// status.
							IServiceAddress[] path_servers = path_info.RootServers;
							bool is_dependent = false;
							foreach (IServiceAddress ps in path_servers) {
								if (ps.Equals(root_service)) {
									is_dependent = true;
									break;
								}
							}
							// If the path is dependent on the root service that changed
							// status
							if (is_dependent) {
								int available_count = 1;
								// Check availability.
								for (int i = 0; i < path_servers.Length; ++i) {
									IServiceAddress paddr = path_servers[i];
									if (!paddr.Equals(address) &&
									    serviceTracker.IsServiceUp(paddr, ServiceType.Root)) {
										++available_count;
									}
								}
								// If less than a majority available,
								if (available_count <= path_servers.Length/2) {
									// Mark the path is unavailable and add to the path info queue,
									path_access.MarkAsNotAvailable();
									desynced_paths.Add(path_info);
								}
							}
						}
					}
				}
			}

			// Add the desync'd paths to the queue,
			lock (loadPathInfoQueue) {
				loadPathInfoQueue.AddRange(desynced_paths);
			}

			// Log message for desyncing,
			if (Logger.IsInterestedIn(Diagnostics.LogLevel.Information)) {
				StringBuilder b = new StringBuilder();
				for (int i = 0; i < desynced_paths.Count; ++i) {
					b.Append(desynced_paths[i].PathName);
					b.Append(", ");
				}
				Logger.Info(String.Format("COMPLETE: desync paths {0} at {1}", new Object[] {b.ToString(), address.ToString()}));
			}
		}
		
		private void CheckPathNameValid(string name) {
			int sz = name.Length;
			for (int i = 0; i < sz; ++i) {
				char c = name[i];
				// If the character is not a letter or digit or lower case, then it's
				// invalid
				if (!Char.IsLetterOrDigit(c) || Char.IsUpper(c))
					throw new ApplicationException("Path name '" + name + "' is invalid, must contain only lower case letters or digits.");
			}
		}
		
		private void CheckPathType(string pathTypeName) {
			try {
				Type pathType = Type.GetType(pathTypeName, true, true);
				Activator.CreateInstance(pathType);
			} catch (TypeLoadException e) {
				throw new ApplicationException("Type not found: " + e.Message);
			} catch (TypeInitializationException e) {
				throw new ApplicationException("Type instantiation exception: " + e.Message);
			} catch (UnauthorizedAccessException e) {
				throw new ApplicationException("Unauthorized Access exception: " + e.Message);
			}
		}
		
		private string GetPathType(string pathName) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			PathAccess pathAccess = GetPathAccess(pathName);
			return pathAccess.PathInfo.PathType;
		}
		
		private void InitPath(PathInfo pathInfo) {
			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathInfo.PathName);

			IPath path;
			lock (pathAccess) {
				try {
					path = pathAccess.Path;
				} catch (TypeLoadException e) {
					throw new ApplicationException("Type not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new ApplicationException("Type instantiation exception: " + e.Message);
				} catch (UnauthorizedAccessException e) {
					throw new ApplicationException("Unauthorized Access exception: " + e.Message);
				}
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache networkCache = ManagerCacheState.GetCache(managerAddresses);
			IPathConnection connection = new PathConnection(this, pathInfo, connector, managerAddresses, networkCache, serviceTracker);

			// Make an initial empty database for the path,
			// TODO: We could keep a cached version of this image, but it's miniscule in size.
			NetworkTreeSystem treeSystem = new NetworkTreeSystem(connector, managerAddresses, networkCache, serviceTracker);
			treeSystem.SetMaxNodeCacheHeapSize(1 * 1024 * 1024);
			DataAddress emptyDbAddr = treeSystem.CreateEmptyDatabase();
			// Publish the empty state to the path,
			connection.Publish(emptyDbAddr);
			// Call the initialize function,
			path.Init(connection);
		}
		
		private DataAddress Commit(PathInfo pathInfo, DataAddress proposal) {
			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathInfo.PathName);

			IPath path;
			lock (pathAccess) {
				try {
					path = pathAccess.Path;
				} catch (TypeLoadException e) {
					throw new CommitFaultException("Type not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new CommitFaultException("Type instantiation exception: " + e.Message);
				} catch (UnauthorizedAccessException e) {
					throw new CommitFaultException("Unauthorized Access exception: " + e.Message);
				}
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache networkCache = ManagerCacheState.GetCache(managerAddresses);
			IPathConnection connection = new PathConnection(this, pathInfo, connector, managerAddresses, networkCache, serviceTracker);

			// Perform the commit,
			return path.Commit(connection, proposal);
		}

		private void BindWithManager(IServiceAddress[] addresses) {
			if (managerAddresses != null)
				throw new ApplicationException("This root service is already bound to a manager service");
			
			try {
				OnBindingWithManager(addresses);
			} catch(Exception e) {
				throw new ApplicationException("Error while binding with manager: " + e.Message, e);
			}
			
			managerAddresses = addresses;
		}
		
		private void UnbindWithManager(IServiceAddress[] addresses) {
			if (managerAddresses == null)
				throw new ApplicationException("This root service is not bound to a manager service");
						
			try {
				OnUnbindingWithManager(addresses);
			} catch (Exception e) {
				throw new ApplicationException("Error while unbinding with manager: " + e.Message, e);
			}
			
			managerAddresses = null;
		}
		
		private PathAccess GetPathAccess(string pathName) {
			// Fetch the lock object for the given path name,
			PathAccess pathAccess;
			lock (lockDb) {
				if (!lockDb.TryGetValue(pathName, out pathAccess)) {
					pathAccess = CreatePathAccess(pathName);
					
					// Put it input the local map
					lockDb[pathName] = pathAccess;
				}
			}
			return pathAccess;
		}

		protected virtual PathAccess CreatePathAccess(string pathName) {
			return new PathAccess(pathName);
		}

		private void OnPost(PathInfo pathInfo, long uid, DataAddress rootNode) {
			// The root servers for the path,
			IServiceAddress[] roots = pathInfo.RootServers;

			// Create the message,
			RequestMessage request = new RequestMessage("notifyNewProposal");
			request.Arguments.Add(pathInfo.PathName);
			request.Arguments.Add(uid);
			request.Arguments.Add(rootNode);

			for (int i = 0; i < roots.Length; ++i) {
				IServiceAddress machine = roots[i];
				// Don't notify this service,
				if (!machine.Equals(address)) {
					// If the service is up in the tracker,
					if (serviceTracker.IsServiceUp(machine, ServiceType.Root)) {
						// Send the message to the service,
						IMessageProcessor processor = connector.Connect(machine, ServiceType.Root);
						Message response = processor.Process(request);
						// If return is a connection fault,
						if (response.HasError && ReplicatedValueStore.IsConnectionFault(response))
							serviceTracker.ReportServiceDownClientReport(machine, ServiceType.Root);
					}
				}
			}
		}

		private void PublishPath(PathInfo pathInfo, DataAddress rootNode) {
			// We can't post if this service is not the root leader,
			if (!pathInfo.RootLeader.Equals(address)) {
				Logger.Error(String.Format("Failed, {0} is not root leader for {1}", new Object[] {address, pathInfo.PathName}));
				throw new ApplicationException("Can't post update, this root service (" + address +
				                               ") is not the root leader for the path: " + pathInfo.PathName);
			}

			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			// Only allow post if complete and synchronized
			pathFile.CheckIsSynchronized();

			// Create a unique time based uid.
			long uid = CreateUID();

			// Post the data address to the path,
			pathFile.PostProposal(uid, rootNode, false);

			// Notify all the root servers of this post,
			OnPost(pathInfo, uid, rootNode);
		}

		private long CreateUID() {
			// TODO: Normalize time over all the servers?
			return DateTime.Now.ToBinary();
		}

		private DataAddress GetPathLast(PathInfo pathInfo) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			// Returns the last entry
			return pathFile.GetPathLast();
		}

		private DataAddress[] GetHistoricalPathRoots(PathInfo pathInfo, DateTime timeStart, DateTime timeEnd) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			// Returns the roots
			return pathFile.GetHistoricalPathRoots(timeStart, timeEnd);
		}

		private DataAddress[] GetPathRootsSince(PathInfo pathInfo, DataAddress root) {
			// Fetch the path access object for the given name.
			PathAccess path_file = GetPathAccess(pathInfo.PathName);

			return path_file.GetPathRootsSince(root);
		}

		private void LoadPathInfo(PathInfo pathInfo) {
			lock (loadPathInfoLock) {
				PathAccess pathAccess = GetPathAccess(pathInfo.PathName);
				pathAccess.SetInitialPathInfo(pathInfo);

				lock (loadPathInfoQueue) {
					if (!loadPathInfoQueue.Contains(pathInfo)) {
						loadPathInfoQueue.Add(pathInfo);
					}
				}

				IServiceAddress[] rootServers = pathInfo.RootServers;

				// Poll all the root servers managing the path,
				PollRootMachines(rootServers);

				// Check availability,
				int availableCount = 0;
				foreach (IServiceAddress addr in rootServers) {
					if (serviceTracker.IsServiceUp(addr, ServiceType.Root)) {
						++availableCount;
					}
				}

				// Majority not available?
				if (availableCount <= (rootServers.Length/2)) {
					Logger.Info("Majority of root servers unavailable, " +
					            "retrying loadPathInfo later");

					// Leave the path info on the load_path_info_queue, therefore it will
					// be retried when the root server is available again,

					return;
				}

				Logger.Info(String.Format("loadPathInfo on path {0} at {1}", pathInfo.PathName, address));

				// Create the local data object if not present,
				pathAccess.Open();

				// Sync counter starts at 1, because we know we are self replicated.
				int syncCounter = 1;

				// Synchronize with each of the available root servers (but not this),
				foreach (IServiceAddress addr in rootServers) {
					if (!addr.Equals(address)) {
						if (serviceTracker.IsServiceUp(addr, ServiceType.Root)) {
							// If we successfully synchronized, increment the counter,
							if (SynchronizePathInfoData(pathAccess, addr)) {
								++syncCounter;
							}
						}
					}
				}

				// Remove from the queue if we successfully sync'd with a majority of
				// the root servers for the path,
				if (syncCounter > pathInfo.RootServers.Length/2) {
					// Replay any proposals that were incoming on the path, and mark the
					// path as synchronized/available,
					pathAccess.MarkAsAvailable();

					lock (loadPathInfoQueue) {
						loadPathInfoQueue.Remove(pathInfo);
					}

					Logger.Info(String.Format("COMPLETE: LoadPathInfo on path {0} at {1}", pathInfo.PathName, address));
				}
			}
		}

		private void OnNewProposal(string pathName, long uid, DataAddress node) {
			// Fetch the path access object for the given name.
			PathAccess pathAccess = GetPathAccess(pathName);

			// Go tell the PathAccess
			pathAccess.OnNewProposal(uid, node);
		}

		private object[] InternalFetchPathDataBundle(string path_name, long uid, DataAddress addr) {
			// Fetch the path access object for the given name.
			PathAccess pathAccess = GetPathAccess(path_name);
			// Init the local data if we need to,
			pathAccess.Open();

			// Fetch 256 entries,
			return pathAccess.FetchPathDataBundle(uid, addr, 256);
		}

		private bool SynchronizePathInfoData(PathAccess path_file, IServiceAddress root_server) {

			// Get the last entry,
			PathRecordEntry last_entry = path_file.GetLastEntry();

			long uid;
			DataAddress daddr;

			if (last_entry == null) {
				uid = 0;
				daddr = null;
			} else {
				uid = last_entry.UID;
				daddr = last_entry.Address;
			}

			while (true) {

				// Fetch a bundle for the path from the root server,
				RequestMessage request = new RequestMessage("internalFetchPathDataBundle");
				request.Arguments.Add(path_file.Name);
				request.Arguments.Add(uid);
				request.Arguments.Add(daddr);

				// Send the command
				IMessageProcessor processor = connector.Connect(root_server, ServiceType.Root);
				Message result = processor.Process(request);

				if (result.HasError) {
					// If it's a connection fault, report the error and return false
					if (ReplicatedValueStore.IsConnectionFault(result)) {
						serviceTracker.ReportServiceDownClientReport(root_server, ServiceType.Root);
						return false;
					}

					throw new ApplicationException(result.ErrorMessage);
				}

				long[] uids = (long[]) result.Arguments[0].Value;
				DataAddress[] dataAddrs = (DataAddress[]) result.Arguments[1].Value;

				// If it's empty, we reached the end so return,
				if (uids == null || uids.Length == 0)
					break;

				// Insert the data
				path_file.AddPathDataEntries(uids, dataAddrs);

				// The last,
				uid = uids[uids.Length - 1];
				daddr = dataAddrs[dataAddrs.Length - 1];

			}

			return true;
		}

		private PathInfo LoadFromManagers(string pathName, int pathInfoVersion) {
			PathInfo pathInfo = null;
			bool foundOne = false;

			// Query all the available manager servers for the path info,
			for (int i = 0; i < managerAddresses.Length && pathInfo == null; ++i) {
				IServiceAddress managerService = managerAddresses[i];
				if (serviceTracker.IsServiceUp(managerService, ServiceType.Manager)) {
					RequestMessage request = new RequestMessage("getPathInfoForPath");
					request.Arguments.Add(pathName);

					IMessageProcessor processor = connector.Connect(managerService, ServiceType.Manager);
					Message result = processor.Process(request);

					if (result.HasError) {
						if (!ReplicatedValueStore.IsConnectionFault(result)) {
							// If it's not a connection fault, we rethrow the error,
							throw new ApplicationException(result.ErrorMessage);
						}
							
						serviceTracker.ReportServiceDownClientReport(managerService, ServiceType.Manager);
					} else {
						foundOne = true;
						pathInfo = (PathInfo) result.Arguments[0].Value;
					}
				}
			}

			// If not received a valid reply from a manager service, generate exception
			if (!foundOne)
				throw new ServiceNotConnectedException("Managers not available");

			// Create and return the path info object,
			return pathInfo;
		}

		private PathInfo GetPathInfo(string pathName, int pathInfoVersion) {
			lock (pathCache) {
				PathInfo rs_path_info;
				if (pathCache.TryGetValue(pathName, out rs_path_info)) {
					rs_path_info = LoadFromManagers(pathName, pathInfoVersion);
					if (rs_path_info == null) {
						throw new InvalidPathInfoException("Unable to load from managers");
					}
					pathCache[pathName] = rs_path_info;
				}
				// If it's out of date,
				if (rs_path_info.Version != pathInfoVersion) {
					throw new InvalidPathInfoException("Path info version out of date");
				}
				return rs_path_info;
			}
		}

		private void InternalSetPathInfo(string pathName, int pathInfoVersion, PathInfo pathInfo) {
			lock (pathCache) {
				pathCache[pathName] = pathInfo;
			}
			// Set the path info in the path access object,
			PathAccess path_file = GetPathAccess(pathName);
			path_file.PathInfo = pathInfo;
		}

		protected virtual void OnBindingWithManager(IServiceAddress[] managerAddresses) {
		}
		
		protected virtual void OnUnbindingWithManager(IServiceAddress[] managerAddresses) {
		}
		
		protected override void OnStop() {
			managerAddresses = null;
		}

		protected override IMessageProcessor CreateProcessor() {
			return new RootServerMessageProcessor(this);
		}
				
		#region PathAccess

		protected class PathAccess {
			private readonly string name;
			private PathInfo pathInfo;
			private bool pathInfoSet;
			private IPath path;
			private Stream accessStream;
			private StrongPagedAccess pagedAccess;
			private DataAddress lastDataAddress;
			private readonly List<object> proposalQueue;
			private readonly object accessLock = new object();
			private bool completeAndSynchronized;

			private byte[] buf = new byte[32];

			public PathAccess(string name) {
				this.name = name;
				proposalQueue = new List<object>();
			}

			public string Name {
				get { return name; }
			}

			public IPath Path {
				get {
					if (path == null) {
						Type pathType = Type.GetType(pathInfo.PathType, true, true);
						path = (IPath)Activator.CreateInstance(pathType);
					}
					return path;
				}
			}

			public PathInfo PathInfo {
				get {
					lock (accessLock) {
						return pathInfo;
					}
				}

				set {
					lock (accessLock) {
						pathInfo = value;
						path = null;
					}
				}
			}

			internal DataAddress LastDataAddress {
				get { return lastDataAddress; }
				set { lastDataAddress = value; }
			}

			public Stream AccessStream {
				get { return accessStream; }
			}

			protected object AccessLock {
				get { return accessLock; }
			}

			internal StrongPagedAccess PagedAccess {
				get { return pagedAccess; }
			}

			protected virtual bool HasLocalData {
				get { return false; }
			}

			internal bool IntHasLocalData {
				get { return HasLocalData; }
			}

			internal bool IsSynchronized {
				get {
					lock (accessLock) {
						return completeAndSynchronized;
					}
				}
			}

			internal void SetInitialPathInfo(PathInfo info) {
				lock (accessLock) {
					if (!pathInfoSet && pathInfo == null) {
						pathInfoSet = true;
						PathInfo = info;
					}
				}
			}

			internal void CheckIsSynchronized() {
				lock (accessLock) {
					if (!completeAndSynchronized) {
						throw new ApplicationException(String.Format("Path {0} on root server {1} is not available", pathInfo.PathName,
						                                             service.ToString()));
					}
				}
			}

			private long BinarySearch(StrongPagedAccess access, long low, long high, long searchUid) {
				while (low <= high) {
					long mid = (low + high) >> 1;

					long pos = mid * RootItemSize;
					long midUid = access.ReadInt64(pos);

					if (midUid < searchUid) { //mid_timestamp < timestamp) {
						low = mid + 1;
					} else if (midUid > searchUid) { //mid_timestamp > timestamp) {
						high = mid - 1;
					} else {
						return mid;
					}
				}
				return -(low + 1);
			}

			internal void InternalOpen() {
				lock (accessLock) {
					Open();
				}
			}

			protected internal virtual void Open() {
			}

			protected void SetAccessStream(Stream stream) {
				if (accessStream != null)
					throw new InvalidOperationException("An access stream to the path data was already set.");

				accessStream = stream;
				pagedAccess = new StrongPagedAccess(accessStream, 1024);
			}

			internal void PostProposal(long uid, DataAddress rootNode, bool ifNotPresent) {
				lock (accessLock) {
					long pos;

					if (ifNotPresent) {
						long setSize = accessStream.Length / RootItemSize;

						// The position of the uid,
						pos = BinarySearch(pagedAccess, 0, setSize - 1, uid);
						if (pos < 0) {
							pos = -(pos + 1);
						}
						// Crudely clear the cache if it has reached a certain threshold,
						pagedAccess.ClearCache(4);

						// Go back some entries
						pos = Math.Max(0, pos - 256);

						// Search,
						while (true) {
							if (pos >= setSize) {
								// End if pos is greater or equal to the size,
								break;
							}
							long readUid = pagedAccess.ReadInt64((pos * RootItemSize) + 0);
							long nodeH = pagedAccess.ReadInt64((pos * RootItemSize) + 8);
							long nodeL = pagedAccess.ReadInt64((pos * RootItemSize) + 16);
							NodeId node = new NodeId(nodeH, nodeL);
							DataAddress data_address = new DataAddress(node);
							// Crudely clear the cache if it's reached a certain threshold,
							pagedAccess.ClearCache(4);

							// Found, so return
							if (uid == readUid && rootNode.Equals(data_address)) {
								return;
							}

							++pos;
						}
					}

					// Not found, so post

					// Write the root node entry to the end of the file,
					Stream f = accessStream;
					pos = f.Length;
					f.Seek(pos, SeekOrigin.Current);
					ByteBuffer.WriteInt8(uid, buf, 0);
					NodeId nodeId = rootNode.Value;
					ByteBuffer.WriteInt8(nodeId.High, buf, 8);
					ByteBuffer.WriteInt8(nodeId.Low, buf, 16);
					f.Write(buf, 0, RootItemSize);
					pagedAccess.InvalidateSection(pos, RootItemSize);

					lastDataAddress = rootNode;
				}
			}

			internal void AddPathDataEntries(long[] uids, DataAddress[] addrs) {
				lock (accessLock) {
					int sz = uids.Length;
					for (int i = 0; i < sz; ++i) {
						long uid = uids[i];
						DataAddress addr = addrs[i];
						// Post the proposal if it's not present
						PostProposal(uid, addr, true);
					}
				}
			}

			internal void MarkAsAvailable() {
				lock (accessLock) {
					int sz = proposalQueue.Count / 2;
					for (int i = 0; i < sz; ++i) {
						// Fetch the uid and root_node pair,
						long uid = (long)proposalQueue[i * 2];
						DataAddress rootNode = (DataAddress)proposalQueue[(i * 2) + 1];

						// Search for the uid, if it's not in the list then we post it to
						// the path.
						PostProposal(uid, rootNode, true);
					}

					// Clear the proposal queue
					proposalQueue.Clear();
					// Complete this path instance,
					completeAndSynchronized = true;
				}
			}

			internal void MarkAsNotAvailable() {
				lock (accessLock) {
					completeAndSynchronized = false;
				}
			}

			internal object[] FetchPathDataBundle(long fromUid, DataAddress fromAddr, int bundleSize) {
				List<PathRecordEntry> entriesAl = new List<PathRecordEntry>(bundleSize);

				// Synchronize over the object
				lock (accessLock) {
					// Check near the end (most likely to be found there),

					long setSize = accessStream.Length / RootItemSize;

					long searchS;
					long found = -1;
					long pos;
					if (fromUid > 0) {
						// If from_uid is real,
						pos = BinarySearch(pagedAccess, 0, setSize - 1, fromUid);
						pos = Math.Max(0, pos - 256);

						searchS = pos;

						// Search to the end,
						while (true) {
							// End condition,
							if (pos >= setSize) {
								break;
							}

							PathRecordEntry entry = GetEntry(pos * RootItemSize);
							if (entry.UID == fromUid && entry.Address.Equals(fromAddr)) {
								// Found!
								found = pos;
								break;
							}
							++pos;
						}

					} else {
						// If from_uid is less than 0, it indicates to fetch the bundle of
						// path entries from the start.
						pos = -1;
						found = 0;
						searchS = 0;
					}

					// If not found,
					if (found < 0) {
						// Try from search_s to 0
						pos = searchS - 1;
						while (true) {
							// End condition,
							if (pos < 0) {
								break;
							}

							PathRecordEntry entry = GetEntry(pos * RootItemSize);
							if (entry.UID == fromUid && entry.Address.Equals(fromAddr)) {
								// Found!
								found = pos;
								break;
							}
							--pos;
						}
					}

					// Go to the next entry,
					++pos;

					// Still not found, or at the end
					if (found < 0 || pos >= setSize) {
						return new Object[] { new long[0], new DataAddress[0] };
					}

					// Fetch a bundle of records from the position
					while (true) {
						// End condition,
						if (pos >= setSize || entriesAl.Count >= bundleSize) {
							break;
						}

						PathRecordEntry entry = GetEntry(pos * RootItemSize);
						entriesAl.Add(entry);

						++pos;
					}
				}

				// Format it as long[] and DataAddress[] arrays
				int sz = entriesAl.Count;
				long[] uids = new long[sz];
				DataAddress[] addrs = new DataAddress[sz];
				for (int i = 0; i < sz; ++i) {
					uids[i] = entriesAl[i].UID;
					addrs[i] = entriesAl[i].Address;
				}
				return new Object[] { uids, addrs };

			}

			private PathRecordEntry GetEntry(long pos) {
				// Synchronize over the object
				lock (accessLock) {
					// Read the long at the position of the root node reference,
					long uid = pagedAccess.ReadInt64(pos);
					long rootNodeIdH = pagedAccess.ReadInt64(pos + 8);
					long rootNodeIdL = pagedAccess.ReadInt64(pos + 16);
					NodeId rootNodeId = new NodeId(rootNodeIdH, rootNodeIdL);
					DataAddress dataAddress = new DataAddress(rootNodeId);
					// Clear the cache if it's over a certain size,
					pagedAccess.ClearCache(4);

					return new PathRecordEntry(uid, dataAddress);
				}
			}

			internal PathRecordEntry GetLastEntry() {
				// Synchronize over the object
				lock (accessLock) {
					// Read the root node entry from the end of the file,
					long pos = accessStream.Length;
					if (pos == 0) {
						// Nothing in the file, so return null
						return null;
					}

					return GetEntry(pos - RootItemSize);
				}
			}

			internal DataAddress GetPathLast() {
				// Synchronize over the object
				lock (accessLock) {

					// Only allow if complete and synchronized
					CheckIsSynchronized();

					if (lastDataAddress == null) {
						// Read the last entry from the file,
						PathRecordEntry r = GetLastEntry();
						if (r == null)
							return null;

						// Cache the DataAddress part,
						lastDataAddress = r.Address;
					}

					return lastDataAddress;
				}
			}

			internal DataAddress[] GetHistoricalPathRoots(DateTime timeStart, DateTime timeEnd) {
				List<DataAddress> nodes = new List<DataAddress>();
				lock (accessLock) {

					// Only allow if complete and synchronized
					CheckIsSynchronized();

					// We perform a binary search for the start and end time in the set of
					// root nodes since the records are ordered by time. Note that the key
					// is a timestamp obtained by System.currentTimeMillis() that could
					// become out of order if the system time is changed or other
					// misc time synchronization oddities. Because of this, we consider the
					// records 'roughly' ordered and it should be noted the result may not
					// be exactly correct.

					long set_size = accessStream.Length / RootItemSize;

					long startP = BinarySearch(pagedAccess, 0, set_size - 1, timeStart.ToBinary());
					long endP = BinarySearch(pagedAccess, 0, set_size - 1, timeEnd.ToBinary());
					if (startP < 0) {
						startP = -(startP + 1);
					}
					if (endP < 0) {
						endP = -(endP + 1);
					}
					// Crudely clear the cache if it has reached a certain threshold,
					pagedAccess.ClearCache(4);

					if (startP >= endP - 1) {
						startP = endP - 2;
						endP = endP + 2;
					}

					startP = Math.Max(0, startP);
					endP = Math.Min(set_size, endP);

					// Return the records,
					while (true) {
						if (startP > endP || startP >= set_size) {
							// End if start has reached the end,
							break;
						}
						long nodeH = pagedAccess.ReadInt64((startP * RootItemSize) + 8);
						long nodeL = pagedAccess.ReadInt64((startP * RootItemSize) + 16);
						NodeId node = new NodeId(nodeH, nodeL);
						nodes.Add(new DataAddress(node));
						// Crudely clear the cache if it's reached a certain threshold,
						pagedAccess.ClearCache(4);

						++startP;
					}
				}

				// Return the nodes array,
				return nodes.ToArray();
			}

			internal DataAddress[] GetPathRootsSince(DataAddress root) {
				// The returned list,
				List<DataAddress> rootList = new List<DataAddress>(6);

				// Synchronize over the object
				lock (accessLock) {
					// Only allow if complete and synchronized
					CheckIsSynchronized();

					bool found = false;

					// Start position at the end of the file,
					long pos = accessStream.Length;

					while (found == false && pos > 0) {
						// Iterate backwards,
						pos = pos - RootItemSize;
						// Read the root node for this record,
						long rootNodeIdH = pagedAccess.ReadInt64(pos + 8);
						long rootNodeIdL = pagedAccess.ReadInt64(pos + 16);
						NodeId rootNodeId = new NodeId(rootNodeIdH, rootNodeIdL);
						// Crudely clear the cache if it's reached a certain threshold,
						pagedAccess.ClearCache(4);
						// Did we find the root node?
						DataAddress rootNodeDa = new DataAddress(rootNodeId);
						if (rootNodeDa.Equals(root)) {
							found = true;
						} else {
							rootList.Add(rootNodeDa);
						}
					}

					// If not found, report error
					if (!found) {
						// Bit of an obscure error message. This basically means we didn't
						// find the root node requested in the path file.
						throw new ApplicationException("Root not found in version list");
					}

					// Return the array as a list,
					return rootList.ToArray();
				}
			}

			internal void OnNewProposal(long uid, DataAddress proposal) {
				lock (accessLock) {
					if (!completeAndSynchronized) {
						// If this isn't complete, we add to the queue
						proposalQueue.Add(uid);
						proposalQueue.Add(proposal);
						return;
					}

					// Otherwise add the entry,
					PostProposal(uid, proposal, false);
				}
			}
		}

		
		#endregion

		internal class PathRecordEntry {
			private readonly long uid;
			private readonly DataAddress address;

			public PathRecordEntry(long uid, DataAddress address) {
				this.uid = uid;
				this.address = address;
			}

			public long UID {
				get { return uid; }
			}

			public DataAddress Address {
				get { return address; }
			}
		}
		
		#region RootServerProcessor

		class RootServerMessageProcessor : IMessageProcessor {
			public RootServerMessageProcessor(RootService service) {
				this.service = service;
			}

			private readonly RootService service;

			private void PublishPath(string pathName, int pathInfoVersion, DataAddress rootNode) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				service.PublishPath(pathInfo, rootNode);
			}

			private DataAddress GetSnapshot(string pathName, int pathInfoVersion) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.GetPathLast(pathInfo);
			}

			private DataAddress[] GetSnapshots(string pathName, int pathInfoVersion, DateTime timeStart, DateTime timeEnd) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.GetHistoricalPathRoots(pathInfo, timeStart, timeEnd);
			}

			private DataAddress Commit(string pathName, int pathInfoVersion, DataAddress proposal) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.Commit(pathInfo, proposal);
			}

			private void InitPath(string pathName, int pathInfoVersion) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				service.InitPath(pathInfo);
			}

			public Message Process(Message request) {
				Message response;
				if (MessageStream.TryProcess(this, request, out response))
					return response;

				// The reply message,

				response = ((RequestMessage) request).CreateResponse();

				try {
					service.CheckErrorState();

					switch (request.Name) {
						case "publishPath": {
							string pathName = request.Arguments[0].ToString();
							int pathVersion = request.Arguments[1].ToInt32();
							DataAddress rootNode = (DataAddress) request.Arguments[2].Value;
							PublishPath(pathName, pathVersion, rootNode);
							response.Arguments.Add(1L);
							break;
						}
						case "getSnapshot": {
							string path = request.Arguments[0].ToString();
							int pathVersion = request.Arguments[1].ToInt32();
							DataAddress address = GetSnapshot(path, pathVersion);
							response.Arguments.Add(address);
							break;
						}
						case "getSnapshots": {
							string path = request.Arguments[0].ToString();
							int pathVersion = request.Arguments[1].ToInt32();
							DateTime start = DateTime.FromBinary(request.Arguments[2].ToInt64());
							DateTime end = DateTime.FromBinary(request.Arguments[3].ToInt64());
							DataAddress[] addresses = GetSnapshots(path, pathVersion, start, end);
							response.Arguments.Add(addresses);
							break;
						}
						case "getCurrentTime": {
								response.Arguments.Add(DateTime.Now.ToUniversalTime().ToBinary());
								break;
							}
						case "getPathType": {
								string pathName = request.Arguments[0].ToString();
								string pathType = service.GetPathType(pathName);
								response.Arguments.Add(pathType);
								break;
							}
						case "checkPathType": {
								string pathType = request.Arguments[0].ToString();
								service.CheckPathType(pathType);
								response.Arguments.Add(1L);
								break;
							}
						case "initPath": {
							string pathName = request.Arguments[0].ToString();
							int pathVersion = request.Arguments[1].ToInt32();
							InitPath(pathName, pathVersion);
							response.Arguments.Add(1L);
							break;
						}
						case "commit": {
								string pathName = request.Arguments[0].ToString();
								int pathVersion = request.Arguments[1].ToInt32();
								DataAddress proposal = (DataAddress)request.Arguments[2].Value;
								DataAddress rootNode = Commit(pathName, pathVersion, proposal);
								response.Arguments.Add(rootNode);
								break;
							}
						case "bindWithManager": {
								IServiceAddress[] managers = (IServiceAddress[])request.Arguments[0].Value;
								service.BindWithManager(managers);
								response.Arguments.Add(1L);
								break;
							}
						case "unbindWithManager": {
								service.UnbindWithManager(service.managerAddresses);
								response.Arguments.Add(1L);
								break;
							}
						case "loadPathInfo": {
							PathInfo pathInfo = (PathInfo) request.Arguments[0].Value;
							service.LoadPathInfo(pathInfo);
							response.Arguments.Add(1L);
							break;
						}
						case "notifyNewProposal": {
							string pathName = request.Arguments[0].ToString();
							long uid = request.Arguments[1].ToInt64();
							DataAddress proposal = (DataAddress) request.Arguments[2].Value;
							service.OnNewProposal(pathName, uid, proposal);
							response.Arguments.Add(1L);
							break;
						}
						case "internalSetPathInfo": {
							string pathName = request.Arguments[0].ToString();
							int pathVersion = request.Arguments[1].ToInt32();
							PathInfo pathInfo = (PathInfo) request.Arguments[2].Value;
							service.InternalSetPathInfo(pathName, pathVersion, pathInfo);
							response.Arguments.Add(1L);
							break;
						}
						case "internalFetchPathDataBundle": {
							string pathName = request.Arguments[0].ToString();
							long uid = request.Arguments[1].ToInt64();
							DataAddress address = (DataAddress) request.Arguments[2].Value;
							object[] r = service.InternalFetchPathDataBundle(pathName, uid, address);
							response.Arguments.Add((long[]) r[0]);
							response.Arguments.Add((DataAddress[]) r[1]);
							break;
						}
						case "poll": {
							response.Arguments.Add(1L);
							break;
						}
						default:
							throw new ApplicationException("Unknown message received: " + request.Name);
					}
				} catch (OutOfMemoryException e) {
					service.Logger.Error("Out of memory!");
					service.SetErrorState(e);
					throw;
				} catch (Exception e) {
					service.Logger.Error("Error while processing", e);
					response.Arguments.Add(new MessageError(e));
				}

				return response;
			}
		}

		#endregion
		
		#region PathConnection
		
		class PathConnection : IPathConnection {
			private readonly RootService service;
			private readonly PathInfo pathInfo;
			private readonly NetworkTreeSystem treeSystem;
			
			public PathConnection(RootService service, PathInfo pathInfo, 
			                      IServiceConnector connector, IServiceAddress[] manager, 
			                      INetworkCache networkCache, ServiceStatusTracker statusTracker) {
				this.service = service;
				this.pathInfo = pathInfo;
				treeSystem = new NetworkTreeSystem(connector, manager, networkCache, statusTracker);
			}
			
			public DataAddress GetSnapshot() {
				try {
					return service.GetPathLast(pathInfo);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public DataAddress[] GetSnapshots(DateTime start, DateTime end) {
				try {
					return service.GetHistoricalPathRoots(pathInfo, start, end);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public DataAddress[] GetSnapshots(DataAddress rootNode) {
				try {
					return service.GetPathRootsSince(pathInfo, rootNode);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public void Publish(DataAddress rootNode) {
				try {
					service.PublishPath(pathInfo, rootNode);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public ITransaction CreateTransaction(DataAddress rootNode) {
				return treeSystem.CreateTransaction(rootNode);
			}
			
			public DataAddress CommitTransaction(ITransaction transaction) {
				return treeSystem.FlushTransaction(transaction);
			}
		}
		
		#endregion

		#region PathStatus

		protected sealed class PathStatus {
			public PathStatus(string pathName, bool deleted) {
				this.pathName = pathName;
				this.deleted = deleted;
			}

			private readonly string pathName;
			private readonly bool deleted;

			public bool IsDeleted {
				get { return deleted; }
			}

			public string PathName {
				get { return pathName; }
			}
		}

		#endregion
	}
}