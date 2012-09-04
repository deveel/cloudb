using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Messaging;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public abstract class RootService : Service {
		private readonly IServiceConnector connector;
		private readonly IServiceAddress address;
		private volatile IServiceAddress[] managerServers;

		private readonly Dictionary<string, PathAccess> lockDb;
		private readonly ServiceStatusTracker serviceTracker;
		private readonly Dictionary<string, PathInfo> pathInfoMap;
		private readonly List<PathInfo> loadPathInfoQueue;

		private readonly object loadPathInfoLock = new object();

		private const int RootItemSize = 24;

		protected RootService(IServiceConnector connector, IServiceAddress address) {
			this.connector = connector;
			this.address = address;

			lockDb = new Dictionary<string, PathAccess>(128);
			pathInfoMap = new Dictionary<string, PathInfo>(128);
			loadPathInfoQueue = new List<PathInfo>(64);

			serviceTracker = new ServiceStatusTracker(connector);
			serviceTracker.StatusChange += ServiceTracker_OnStatusChange;
		}

		private void ServiceTracker_OnStatusChange(object sender, ServiceStatusEventArgs args) {
			// If it's a manager service, and the new status is UP
			if (args.ServiceType == ServiceType.Manager &&
			    args.NewStatus == ServiceStatus.Up) {
				// Init and load all pending paths,
				ProcessInitQueue();
				LoadAllPendingPaths();
			}

			// If it's a root service, and the new status is UP
			if (args.ServiceType == ServiceType.Root) {
				if (args.NewStatus == ServiceStatus.Up) {
					// Load all pending paths,
					LoadAllPendingPaths();
				}
					// If a root server went down,
				else if (args.NewStatus == ServiceStatus.DownShutdown ||
				         args.NewStatus == ServiceStatus.DownClientReport ||
				         args.NewStatus == ServiceStatus.DownHeartbeat) {
					// Scan the paths managed by this root server and desynchronize
					// any not connected to a majority of servers.
					DesyncPathsDependentOn(address);
				}
			}
		}

		protected object PathInitLock {
			get { return loadPathInfoQueue; }
		}

		protected virtual void ProcessInitQueue() {
		}

		protected void AddPathToQueue(PathInfo pathInfo) {
			loadPathInfoQueue.Add(pathInfo);
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Root; }
		}

		public IServiceAddress[] ManagerServices {
			get { return managerServers; }
			protected set { managerServers = value; }
		}

		protected override void OnStart() {
			// Schedule the load on the timer thread,
			new Timer(LoadTask, null, 2000, Timeout.Infinite);
		}

		private void LoadTask(object state) {
			// Load the paths if we can,
			ProcessInitQueue();
			LoadAllPendingPaths();
		}

		protected override void OnStop() {
			managerServers = null;
		}

		private void LoadAllPendingPaths() {
			// Make a copy of the pending path info loads,
			List<PathInfo> piList = new List<PathInfo>(64);
			lock (loadPathInfoQueue) {
				foreach (PathInfo pi in loadPathInfoQueue) {
					piList.Add(pi);
				}
			}
			// Do the load operation on the pending,
			try {
				foreach (PathInfo pi in piList) {
					LoadPathInfo(pi);
				}
			} catch (IOException e) {
				Logger.Error("IO Error", e);
			}
		}

		private void CheckPathNameValid(String name) {
			int sz = name.Length;
			bool invalid = false;
			for (int i = 0; i < sz; ++i) {
				char c = name[i];
				// If the character is not a letter or digit or lower case, then it's
				// invalid
				if (!Char.IsLetterOrDigit(c) || 
					Char.IsUpper(c)) {
					invalid = true;
				}
			}

			// Exception if invalid,
			if (invalid) {
				throw new ApplicationException("Path name '" + name +
						"' is invalid, must contain only lower case letters or digits.");
			}
		}

		private void PollAllRootMachines(IServiceAddress[] machines) {
			// Create the message,
			MessageStream outputStream = new MessageStream();
			outputStream.AddMessage(new Message("poll", "RSPoll"));

			for (int i = 0; i < machines.Length; ++i) {
				IServiceAddress machine = machines[i];
				// If the service is up in the tracker,
				if (serviceTracker.IsServiceUp(machine, ServiceType.Root)) {
					// Poll it to see if it's really up.
					// Send the poll to the service,
					IMessageProcessor processor = connector.Connect(machine, ServiceType.Root);
					IEnumerable<Message> inputStream = processor.Process(outputStream);
					// If return is a connection fault,
					foreach (Message m in inputStream) {
						if (m.HasError && 
							ReplicatedValueStore.IsConnectionFault(m)) {
							serviceTracker.ReportServiceDownClientReport(machine, ServiceType.Root);
						}
					}
				}
			}
		}

		private void DesyncPathsDependentOn(IServiceAddress rootService) {
			List<PathInfo> desyncedPaths = new List<PathInfo>(64);

			lock (lockDb) {
				// The set of all paths,
				foreach (KeyValuePair<string, PathAccess> inPath in lockDb) {
					// Get the PathInfo for the path,
					PathAccess pathAccess = inPath.Value;
					// We have to be available to continue,
					if (pathAccess.IsSynchronized) {
						PathInfo pathInfo = pathAccess.PathInfo;
						// If it's null, we haven't finished initialization yet,
						if (pathInfo != null) {
							// Check if the path info is dependent on the service that changed
							// status.
							IServiceAddress[] pathServers = pathInfo.RootServers;
							bool isDependent = false;
							foreach (IServiceAddress ps in pathServers) {
								if (ps.Equals(rootService)) {
									isDependent = true;
									break;
								}
							}
							// If the path is dependent on the root service that changed
							// status
							if (isDependent) {
								int availableCount = 1;
								// Check availability.
								for (int i = 0; i < pathServers.Length; ++i) {
									IServiceAddress paddr = pathServers[i];
									if (!paddr.Equals(address) &&
									    serviceTracker.IsServiceUp(paddr, ServiceType.Root)) {
										++availableCount;
									}
								}
								// If less than a majority available,
								if (availableCount <= pathServers.Length/2) {
									// Mark the path is unavailable and add to the path info queue,
									pathAccess.MarkAsNotAvailable();
									desyncedPaths.Add(pathInfo);
								}
							}
						}
					}
				}
			}

			// Add the desync'd paths to the queue,
			lock (loadPathInfoQueue) {
				loadPathInfoQueue.AddRange(desyncedPaths);
			}

			// Log message for desyncing,
			if (Logger.IsInterestedIn(Diagnostics.LogLevel.Information)) {
				StringBuilder b = new StringBuilder();
				for (int i = 0; i < desyncedPaths.Count; ++i) {
					b.Append(desyncedPaths[i].PathName);
					b.Append(", ");
				}
				Logger.Info(String.Format("COMPLETE: desync paths {0} at {1}", b, address));
			}
		}

		protected abstract PathAccess CreatePathAccesss(string pathName);

		private PathAccess GetPathAccess(String pathName) {

			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the lock object for the given path name,

			lock (lockDb) {
				PathAccess pathFile;
				if (!lockDb.TryGetValue(pathName, out pathFile)) {
					pathFile = CreatePathAccesss(pathName);
					lockDb[pathName] = pathFile;
				}
				return pathFile;
			}
		}

		private void NotifyAllRootServersOfPost(PathInfo pathInfo, long uid, DataAddress rootNode) {
			// The root servers for the path,
			IServiceAddress[] roots = pathInfo.RootServers;

			// Create the message,
			MessageStream outputStream = new MessageStream();
			outputStream.AddMessage(new Message("notifyNewProposal", pathInfo.PathName, uid, rootNode));

			for (int i = 0; i < roots.Length; ++i) {
				IServiceAddress machine = roots[i];
				// Don't notify this service,
				if (!machine.Equals(address)) {
					// If the service is up in the tracker,
					if (serviceTracker.IsServiceUp(machine, ServiceType.Root)) {
						// Send the message to the service,
						IMessageProcessor processor = connector.Connect(machine, ServiceType.Root);
						IEnumerable<Message> inputStream = processor.Process(outputStream);
						// If return is a connection fault,
						foreach (Message m in inputStream) {
							if (m.HasError && 
								ReplicatedValueStore.IsConnectionFault(m)) {
								serviceTracker.ReportServiceDownClientReport(machine, ServiceType.Root);
							}
						}
					}
				}
			}
		}

		private void PostToPath(PathInfo pathInfo, DataAddress rootNode) {
			// We can't post if this service is not the root leader,
			if (!pathInfo.RootLeader.Equals(address)) {
				Logger.Error(String.Format("Failed, {0} is not root leader for {1}", address, pathInfo.PathName));
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
			pathFile.PostProposalToPath(uid, rootNode);

			// Notify all the root servers of this post,
			NotifyAllRootServersOfPost(pathInfo, uid, rootNode);
		}

		private long CreateUID() {
			return DateTimeUtil.CurrentTimeMillis();
		}

		private DataAddress GetPathLast(PathInfo pathInfo) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			// Returns the last entry
			return pathFile.GetPathLast();
		}

		private DataAddress[] GetHistoricalPathRoots(PathInfo pathInfo, long timeStart, long timeEnd) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			// Returns the roots
			return pathFile.GetHistoricalPathRoots(timeStart, timeEnd);
		}

		private DataAddress[] GetPathRootsSince(PathInfo pathInfo, DataAddress root) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			return pathFile.GetPathRootsSince(root);
		}

		private string GetPathType(string pathName) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathName);

			// Return the processor name
			return pathFile.PathInfo.PathType;
		}

		private DataAddress PerformCommit(PathInfo pathInfo, DataAddress proposal) {

			IServiceAddress[] manSrvs = managerServers;

			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			IPath pathFunction;
			try {
				pathFunction = pathFile.Path;
			} catch (TypeLoadException e) {
				throw new CommitFaultException(String.Format("Type not found: {0}", e.Message));
			} catch (TypeInitializationException e) {
				throw new CommitFaultException(String.Format("Type instantiation exception: {0}", e.Message));
			} catch (AccessViolationException e) {
				throw new CommitFaultException(String.Format("Illegal Access exception: {0}", e.Message));
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache localNetCache = MachineState.GetCacheForManager(manSrvs);
			IPathConnection connection = new PathConnection(this, pathInfo, connector, manSrvs, localNetCache, serviceTracker);

			// Perform the commit,
			return pathFunction.Commit(connection, proposal);
		}

		private String iGetSnapshotStats(PathInfo pathInfo, DataAddress snapshot) {
			IServiceAddress[] manSrvs = managerServers;

			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			IPath pathFunction;
			try {
				pathFunction = pathFile.Path;
			} catch (TypeLoadException e) {
				throw new CommitFaultException(String.Format("Type not found: {0}", e.Message));
			} catch (TypeInitializationException e) {
				throw new CommitFaultException(String.Format("Type instantiation exception: {0}", e.Message));
			} catch (AccessViolationException e) {
				throw new CommitFaultException(String.Format("Illegal Access exception: {0}", e.Message));
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache localNetCache = MachineState.GetCacheForManager(manSrvs);
			IPathConnection connection = new PathConnection(this, pathInfo, connector, manSrvs, localNetCache, serviceTracker);

			// Generate and return the stats string,
			return pathFunction.GetStats(connection, snapshot);
		}

		private String iGetPathStats(PathInfo pathInfo) {
			return iGetSnapshotStats(pathInfo, GetPathLast(pathInfo));
		}

		private static void CheckPathType(string typeString) {
			try {
				Type pathType = Type.GetType(typeString, true, true);
				IPath path = (IPath) Activator.CreateInstance(pathType);
			} catch (TypeLoadException e) {
				throw new CommitFaultException(String.Format("Type not found: {0}", e.Message));
			} catch (TypeInitializationException e) {
				throw new CommitFaultException(String.Format("Type instantiation exception: {0}", e.Message));
			} catch (AccessViolationException e) {
				throw new CommitFaultException(String.Format("Illegal Access exception: {0}", e.Message));
			}
		}

		private void InitPath(PathInfo pathInfo) {
			IServiceAddress[] manSrvs = managerServers;

			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathInfo.PathName);

			IPath pathFunction;
			try {
				pathFunction = pathFile.Path;
			} catch (TypeLoadException e) {
				throw new CommitFaultException(String.Format("Type not found: {0}", e.Message));
			} catch (TypeInitializationException e) {
				throw new CommitFaultException(String.Format("Type instantiation exception: {0}", e.Message));
			} catch (AccessViolationException e) {
				throw new CommitFaultException(String.Format("Illegal Access exception: {0}", e.Message));
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache localNetCache = MachineState.GetCacheForManager(manSrvs);
			IPathConnection connection = new PathConnection(this, pathInfo, connector, manSrvs, localNetCache, serviceTracker);

			// Make an initial empty database for the path,
			// PENDING: We could keep a cached version of this image, but it's
			//   miniscule in size.
			NetworkTreeSystem treeSystem = new NetworkTreeSystem(connector, manSrvs, localNetCache, serviceTracker);
			treeSystem.NodeHeapMaxSize = 1*1024*1024;
			DataAddress emptyDbAddr = treeSystem.CreateDatabase();
			// Publish the empty state to the path,
			connection.Publish(emptyDbAddr);
			// Call the initialize function,
			pathFunction.Init(connection);
		}

		private void LoadPathInfo(PathInfo pathInfo) {
			lock (loadPathInfoLock) {
				PathAccess pathFile = GetPathAccess(pathInfo.PathName);
				pathFile.SetInitialPathInfo(pathInfo);

				lock (loadPathInfoQueue) {
					if (!loadPathInfoQueue.Contains(pathInfo)) {
						loadPathInfoQueue.Add(pathInfo);
					}
				}

				IServiceAddress[] rootServers = pathInfo.RootServers;

				// Poll all the root servers managing the path,
				PollAllRootMachines(rootServers);

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
				pathFile.OpenLocalData();

				// Sync counter starts at 1, because we know we are self replicated.
				int syncCounter = 1;

				// Synchronize with each of the available root servers (but not this),
				foreach (IServiceAddress addr in rootServers) {
					if (!addr.Equals(address)) {
						if (serviceTracker.IsServiceUp(addr, ServiceType.Root)) {
							bool success = SynchronizePathInfoData(pathFile, addr);
							// If we successfully synchronized, increment the counter,
							if (success) {
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
					pathFile.MarkAsAvailable();

					lock (loadPathInfoQueue) {
						loadPathInfoQueue.Remove(pathInfo);
					}

					Logger.Info(String.Format("COMPLETE: loadPathInfo on path {0} at {1}", pathInfo.PathName, address));
				}
			}
		}

		private void NotifyNewProposal(String pathName, long uid, DataAddress node) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathName);

			// Go tell the PathAccess
			pathFile.NotifyNewProposal(uid, node);
		}

		private object[] InternalFetchPathDataBundle(string pathName, long uid, DataAddress addr) {
			// Fetch the path access object for the given name.
			PathAccess pathFile = GetPathAccess(pathName);
			// Init the local data if we need to,
			pathFile.OpenLocalData();

			// Fetch 256 entries,
			return pathFile.FetchPathDataBundle(uid, addr, 256);
		}

		private bool SynchronizePathInfoData(PathAccess pathFile, IServiceAddress rootServer) {
			// Get the last entry,
			PathRecordEntry lastEntry = pathFile.GetLastEntry();

			long uid;
			DataAddress daddr;

			if (lastEntry == null) {
				uid = 0;
				daddr = null;
			} else {
				uid = lastEntry.Uid;
				daddr = lastEntry.Address;
			}

			while (true) {
				// Fetch a bundle for the path from the root server,
				MessageStream outputStream = new MessageStream();
				outputStream.AddMessage(new Message("internalFetchPathDataBundle", pathFile.PathName, uid, daddr));

				// Send the command
				IMessageProcessor processor = connector.Connect(rootServer, ServiceType.Root);
				IEnumerable<Message> result = processor.Process(outputStream);

				long[] uids = null;
				DataAddress[] dataAddrs = null;

				foreach (Message m in result) {
					if (m.HasError) {
						// If it's a connection fault, report the error and return false
						if (ReplicatedValueStore.IsConnectionFault(m)) {
							serviceTracker.ReportServiceDownClientReport(rootServer, ServiceType.Root);
							return false;
						}

						throw new ApplicationException(m.ErrorMessage);
					}

					uids = (long[]) m.Arguments[0].Value;
					dataAddrs = (DataAddress[]) m.Arguments[1].Value;
				}

				// If it's empty, we reached the end so return,
				if (uids == null || uids.Length == 0) {
					break;
				}

				// Insert the data
				pathFile.AddPathDataEntries(uids, dataAddrs);

				// The last,
				uid = uids[uids.Length - 1];
				daddr = dataAddrs[dataAddrs.Length - 1];

			}

			return true;
		}

		protected virtual void OnManagersSet(IServiceAddress[] addresses) {
			managerServers = addresses;			
		}

		protected virtual void OnManagersClear() {
			managerServers = new IServiceAddress[0];
		}


		protected PathInfo LoadFromManagers(string pathName, int pathInfoVersion) {
			IServiceAddress[] manSrvs = managerServers;

			PathInfo pathInfo = null;
			bool foundOne = false;

			// Query all the available manager servers for the path info,
			for (int i = 0; i < manSrvs.Length && pathInfo == null; ++i) {
				IServiceAddress managerService = manSrvs[i];
				if (serviceTracker.IsServiceUp(managerService, ServiceType.Manager)) {
					MessageStream outputStream = new MessageStream();
					outputStream.AddMessage(new Message("getPathInfoForPath", pathName));

					IMessageProcessor processor = connector.Connect(managerService, ServiceType.Manager);
					IEnumerable<Message> result = processor.Process(outputStream);
					Message lastM = null;
					foreach (Message m in result) {
						if (m.HasError) {
							if (!ReplicatedValueStore.IsConnectionFault(m))
								// If it's not a connection fault, we rethrow the error,
								throw new ApplicationException(m.ErrorMessage);

							serviceTracker.ReportServiceDownClientReport(managerService, ServiceType.Manager);
						} else {
							lastM = m;
							foundOne = true;
						}
					}

					if (lastM != null) {
						pathInfo = (PathInfo)lastM.Arguments[0].Value;
					}
				}
			}

			// If not received a valid reply from a manager service, generate exception
			if (foundOne == false)
				throw new ServiceNotConnectedException("Managers not available");

			// Create and return the path info object,
			return pathInfo;
		}

		private PathInfo GetPathInfo(String pathName, int pathInfoVersion) {
			lock (pathInfoMap) {
				PathInfo rsPathInfo;
				if (!pathInfoMap.TryGetValue(pathName, out rsPathInfo)) {
					rsPathInfo = LoadFromManagers(pathName, pathInfoVersion);
					if (rsPathInfo == null)
						throw new InvalidPathInfoException("Unable to load from managers");

					pathInfoMap[pathName] = rsPathInfo;
				}
				// If it's out of date,
				if (rsPathInfo.VersionNumber != pathInfoVersion)
					throw new InvalidPathInfoException("Path info version out of date");

				return rsPathInfo;
			}
		}

		private void InternalSetPathInfo(string pathName, int pathInfoVersion, PathInfo pathInfo) {
			lock (pathInfoMap) {
				pathInfoMap[pathName] = pathInfo;
			}

			// Set the path info in the path access object,
			PathAccess pathFile = GetPathAccess(pathName);
			pathFile.PathInfo = pathInfo;
		}


		protected override IMessageProcessor CreateProcessor() {
			return new MessageProcessor(this);
		}

		#region MessageProcessor

		class MessageProcessor : IMessageProcessor {
			private readonly RootService service;

			public MessageProcessor(RootService service) {
				this.service = service;
			}

			private void PublishPath(string pathName, int pathInfoVersion, DataAddress rootNode) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				service.PostToPath(pathInfo, rootNode);
			}

			private DataAddress GetPathNow(string pathName, int pathInfoVersion) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.GetPathLast(pathInfo);
			}

			private DataAddress[] GetPathHistorical(string pathName, int pathInfoVersion, long timeStart, long timeEnd) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.GetHistoricalPathRoots(pathInfo, timeStart, timeEnd);
			}

			private void Initialize(String pathName, int pathInfoVersion) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				service.InitPath(pathInfo);
			}

			private DataAddress Commit(string pathName, int pathInfoVersion, DataAddress proposal) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.PerformCommit(pathInfo, proposal);
			}

			private string GetSnapshotStats(string pathName, int pathInfoVersion, DataAddress address) {

				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.iGetSnapshotStats(pathInfo, address);
			}

			private String GetPathStats(string pathName, int pathInfoVersion) {
				// Find the PathInfo object from the path_info_version. If the path
				// version is out of date then an exception is generated.
				PathInfo pathInfo = service.GetPathInfo(pathName, pathInfoVersion);

				return service.iGetPathStats(pathInfo);
			}


			public IEnumerable<Message> Process(IEnumerable<Message> stream) {
				// The reply message,
				MessageStream replyMessage = new MessageStream();

				// The messages in the stream,
				foreach (Message m in stream) {
					try {
						service.CheckErrorState();
						// publishPath(string, long, DataAddress)
						if (m.Name.Equals("publishPath")) {
							PublishPath((String) m.Arguments[0].Value, (int) m.Arguments[1].Value, (DataAddress) m.Arguments[2].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// DataAddress getPathNow(String, long)
						else if (m.Name.Equals("getPathNow")) {
							DataAddress dataAddress = GetPathNow((string)m.Arguments[0].Value, (int)m.Arguments[1].Value);
							replyMessage.AddMessage(new Message(dataAddress));
						}
							// DataAddress[] getPathHistorical(string, long, long, long)
						else if (m.Name.Equals("getPathHistorical")) {
							DataAddress[] dataAddresses = GetPathHistorical((string) m.Arguments[0].Value, (int) m.Arguments[1].Value,
							                                                 (long) m.Arguments[2].Value, (long) m.Arguments[3].Value);
							replyMessage.AddMessage(new Message(new object[] {dataAddresses}));
						}
							// long getCurrentTimeMillis()
						else if (m.Name.Equals("getCurrentTimeMillis")) {
							long timeMillis = DateTimeUtil.CurrentTimeMillis();
							replyMessage.AddMessage(new Message(timeMillis));
						}

						// commit(string, long, DataAddress)
						else if (m.Name.Equals("commit")) {
							DataAddress result = Commit((string) m.Arguments[0].Value, (int) m.Arguments[1].Value,
							                            (DataAddress) m.Arguments[2].Value);

							replyMessage.AddMessage(new Message(result));
						}

						// getPathType(String path)
						else if (m.Name.Equals("getPathType")) {
							string pathType = service.GetPathType((string)m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(pathType));
						}

						// initialize(string, long)
						else if (m.Name.Equals("initialize")) {
							Initialize((string)m.Arguments[0].Value, (int)m.Arguments[1].Value);
							replyMessage.AddMessage(new Message(1L));
						}

						// getPathStats(string, long)
						else if (m.Name.Equals("getPathStats")) {
							String stats = GetPathStats((string)m.Arguments[0].Value, (int)m.Arguments[1].Value);
							replyMessage.AddMessage(new Message(stats));
						}
							// getSnapshotStats(string, long, DataAddress)
						else if (m.Name.Equals("getSnapshotStats")) {
							String stats = GetSnapshotStats((string)m.Arguments[0].Value, (int)m.Arguments[1].Value, (DataAddress)m.Arguments[2].Value);
							replyMessage.AddMessage(new Message(stats));
						}

						// checkCPathType(string)
						else if (m.Name.Equals("checkPathType")) {
							CheckPathType((string)m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}

						// informOfManagers(ServiceAddress[] manager_servers)
						else if (m.Name.Equals("informOfManagers")) {
							service.OnManagersSet((IServiceAddress[])m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// clearOfManagers()
						else if (m.Name.Equals("clearOfManagers")) {
							service.OnManagersClear();
							replyMessage.AddMessage(new Message(1L));
						}

						// loadPathInfo(PathInfo)
						else if (m.Name.Equals("loadPathInfo")) {
							service.LoadPathInfo((PathInfo)m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// notifyNewProposal(string, long, DataAddress)
						else if (m.Name.Equals("notifyNewProposal")) {
							service.NotifyNewProposal((string) m.Arguments[0].Value, (long) m.Arguments[1].Value,
							                          (DataAddress) m.Arguments[2].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// internalSetPathInfo(String path_name, long ver, PathInfo path_info)
						else if (m.Name.Equals("internalSetPathInfo")) {
							service.InternalSetPathInfo((string) m.Arguments[0].Value, (int) m.Arguments[1].Value,
							                            (PathInfo) m.Arguments[2].Value);
							replyMessage.AddMessage(new Message(1L));
						}
							// internalFetchPathDataBundle(String path_name,
							//                             long uid, DataAddress addr)
						else if (m.Name.Equals("internalFetchPathDataBundle")) {
							object[] r = service.InternalFetchPathDataBundle((string) m.Arguments[0].Value, (long) m.Arguments[1].Value,
							                                                 (DataAddress) m.Arguments[2].Value);
							replyMessage.AddMessage(new Message(r));
						}

						// poll()
						else if (m.Name.Equals("poll")) {
							replyMessage.AddMessage(new Message(1));
						} else {
							throw new ApplicationException("Unknown command: " + m.Name);
						}

					} catch (OutOfMemoryException e) {
						service.Logger.Error("Memory Error", e);
						service.SetErrorState(e);
						throw;
					} catch (Exception e) {
						service.Logger.Error("Exception during process", e);
						replyMessage.AddMessage(new Message(new MessageError(e)));
					}
				}

				return replyMessage;
			}
		}

		#endregion

		#region PathAccess

		protected abstract class PathAccess {
			private readonly RootService service;
			private readonly String pathName;
			private PathInfo pathInfo;
			private IPath pathFunction;
			private Stream internalFile;
			private StrongPagedAccess pagedFile;

			private DataAddress lastDataAddress;

			private readonly List<object> proposalQueue;


			private readonly object accessLock = new Object();


			private bool completeAndSynchronized;
			private bool pathInfoSet;


			private byte[] buf = new byte[32];

			public PathAccess(RootService service, string pathName) {
				this.service = service;
				this.pathName = pathName;

				proposalQueue = new List<object>();
			}

			protected RootService RootService {
				get { return service; }
			}

			private long BinarySearch(StrongPagedAccess access, long low, long high, long searchUid) {
				while (low <= high) {
					long mid = (low + high) >> 1;

					long pos = mid*RootItemSize;
					long midUid = access.ReadInt64(pos);

					if (midUid < searchUid) {
						//mid_timestamp < timestamp) {
						low = mid + 1;
					} else if (midUid > searchUid) {
						//mid_timestamp > timestamp) {
						high = mid - 1;
					} else {
						return mid;
					}
				}
				return -(low + 1);
			}

			public bool IsSynchronized {
				get {
					lock (accessLock) {
						return completeAndSynchronized;
					}
				}
			}

			internal void CheckIsSynchronized() {
				lock (accessLock) {
					if (!completeAndSynchronized) {
						throw new ApplicationException(String.Format("Path {0} on root server {1} is not available",
						                                             pathInfo.PathName, service.address));
					}
				}
			}

			internal void SetInitialPathInfo(PathInfo value) {
				lock (accessLock) {
					if (pathInfoSet == false &&
					    pathInfo == null) {
						pathInfoSet = true;
						PathInfo = value;
					}
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
						pathFunction = null;
					}
				}
			}

			protected abstract Stream CreatePathStream();

			public void OpenLocalData() {
				lock (accessLock) {
					if (internalFile == null) {
						internalFile = CreatePathStream();
						pagedFile = new StrongPagedAccess(internalFile, 1024);
					}
				}
			}

			public void PostProposalToPath(long uid, DataAddress rootNode) {
				// Synchronize over the random access path file,
				lock (accessLock) {

					// Write the root node entry to the end of the file,
					Stream f = internalFile;
					long pos = f.Length;
					f.Seek(pos, SeekOrigin.Begin);
					ByteBuffer.WriteInt8(uid, buf, 0);
					NodeId nodeId = rootNode.Value;
					ByteBuffer.WriteInt8(nodeId.High, buf, 8);
					ByteBuffer.WriteInt8(nodeId.Low, buf, 16);
					f.Write(buf, 0, RootItemSize);
					pagedFile.InvalidateSection(pos, RootItemSize);

					lastDataAddress = rootNode;
				}
			}

			private void PostProposalIfNotPresent(long uid, DataAddress rootNode) {
				lock (accessLock) {

					long setSize = internalFile.Length/RootItemSize;

					// The position of the uid,
					long pos = BinarySearch(pagedFile, 0, setSize - 1, uid);
					if (pos < 0) {
						pos = -(pos + 1);
					}
					// Crudely clear the cache if it has reached a certain threshold,
					pagedFile.ClearCache(4);

					// Go back some entries
					pos = Math.Max(0, pos - 256);

					// Search,
					while (true) {
						if (pos >= setSize)
							// End if pos is greater or equal to the size,
							break;

						long readUid = pagedFile.ReadInt64((pos*RootItemSize) + 0);
						long nodeH = pagedFile.ReadInt64((pos*RootItemSize) + 8);
						long nodeL = pagedFile.ReadInt64((pos*RootItemSize) + 16);
						NodeId node = new NodeId(nodeH, nodeL);
						DataAddress dataAddress = new DataAddress(node);

						// Crudely clear the cache if it's reached a certain threshold,
						pagedFile.ClearCache(4);

						// Found, so return
						if (uid == readUid &&
						    rootNode.Equals(dataAddress)) {
							return;
						}

						++pos;
					}

					// Not found, so post
					PostProposalToPath(uid, rootNode);
				}
			}

			public void AddPathDataEntries(long[] uids, DataAddress[] addrs) {
				lock (accessLock) {
					int sz = uids.Length;
					for (int i = 0; i < sz; ++i) {
						long uid = uids[i];
						DataAddress addr = addrs[i];
						// Post the proposal if it's not present
						PostProposalIfNotPresent(uid, addr);
					}
				}
			}

			public void MarkAsAvailable() {
				lock (accessLock) {
					int sz = proposalQueue.Count/2;
					for (int i = 0; i < sz; ++i) {
						// Fetch the uid and root_node pair,
						long uid = (long) proposalQueue[(i*2)];
						DataAddress root_node = (DataAddress) proposalQueue[(i*2) + 1];

						// Search for the uid, if it's not in the list then we post it to
						// the path.
						PostProposalIfNotPresent(uid, root_node);
					}

					// Clear the proposal queue
					proposalQueue.Clear();
					// Complete this path instance,
					completeAndSynchronized = true;

				}
			}

			public void MarkAsNotAvailable() {
				lock (accessLock) {
					completeAndSynchronized = false;
				}
			}

			public object[] FetchPathDataBundle(long fromUid, DataAddress fromAddr, int bundleSize) {

				List<PathRecordEntry> entriesList = new List<PathRecordEntry>(bundleSize);

				// Synchronize over the object
				lock (accessLock) {
					// Check near the end (most likely to be found there),

					long setSize = internalFile.Length/RootItemSize;

					long searchS;
					long found = -1;
					long pos;
					if (fromUid > 0) {
						// If from_uid is real,
						pos = BinarySearch(pagedFile, 0, setSize - 1, fromUid);
						pos = Math.Max(0, pos - 256);

						searchS = pos;

						// Search to the end,
						while (true) {
							// End condition,
							if (pos >= setSize) {
								break;
							}

							PathRecordEntry entry = GetEntryAt(pos*RootItemSize);
							if (entry.Uid == fromUid && entry.Address.Equals(fromAddr)) {
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

							PathRecordEntry entry = GetEntryAt(pos*RootItemSize);
							if (entry.Uid == fromUid && entry.Address.Equals(fromAddr)) {
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
						return new Object[] {new long[0], new DataAddress[0]};
					}

					// Fetch a bundle of records from the position
					while (true) {
						// End condition,
						if (pos >= setSize || entriesList.Count >= bundleSize) {
							break;
						}

						PathRecordEntry entry = GetEntryAt(pos*RootItemSize);
						entriesList.Add(entry);

						++pos;
					}
				}

				// Format it as long[] and DataAddress[] arrays
				int sz = entriesList.Count;
				long[] uids = new long[sz];
				DataAddress[] addrs = new DataAddress[sz];
				for (int i = 0; i < sz; ++i) {
					uids[i] = entriesList[i].Uid;
					addrs[i] = entriesList[i].Address;
				}
				return new Object[] {uids, addrs};

			}

			private PathRecordEntry GetEntryAt(long pos) {
				// Synchronize over the object
				lock (accessLock) {
					// Read the long at the position of the root node reference,
					long uid = pagedFile.ReadInt64(pos);
					long rootNodeRefH = pagedFile.ReadInt64(pos + 8);
					long rootNodeRefL = pagedFile.ReadInt64(pos + 16);
					NodeId rootNodeId = new NodeId(rootNodeRefH, rootNodeRefL);
					DataAddress dataAddress = new DataAddress(rootNodeId);
					// Clear the cache if it's over a certain size,
					pagedFile.ClearCache(4);

					return new PathRecordEntry(uid, dataAddress);
				}
			}

			internal PathRecordEntry GetLastEntry() {
				// Synchronize over the object
				lock (accessLock) {
					// Read the root node entry from the end of the file,
					long pos = internalFile.Length;
					if (pos == 0)
						// Nothing in the file, so return null
						return null;

					return GetEntryAt(pos - RootItemSize);
				}
			}

			public DataAddress GetPathLast() {
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

			public DataAddress[] GetHistoricalPathRoots(long timeStart, long timeEnd) {
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

					long setSize = internalFile.Length/RootItemSize;

					long startP = BinarySearch(pagedFile, 0, setSize - 1, timeStart);
					long endP = BinarySearch(pagedFile, 0, setSize - 1, timeEnd);
					if (startP < 0) {
						startP = -(startP + 1);
					}
					if (endP < 0) {
						endP = -(endP + 1);
					}
					// Crudely clear the cache if it has reached a certain threshold,
					pagedFile.ClearCache(4);

					if (startP >= endP - 1) {
						startP = endP - 2;
						endP = endP + 2;
					}

					startP = Math.Max(0, startP);
					endP = Math.Min(setSize, endP);

					// Return the records,
					while (true) {
						if (startP > endP || startP >= setSize)
							// End if start has reached the end,
							break;

						long nodeH = pagedFile.ReadInt64((startP*RootItemSize) + 8);
						long nodeL = pagedFile.ReadInt64((startP*RootItemSize) + 16);
						NodeId node = new NodeId(nodeH, nodeL);
						nodes.Add(new DataAddress(node));
						// Crudely clear the cache if it's reached a certain threshold,
						pagedFile.ClearCache(4);

						++startP;
					}
				}

				// Return the nodes array,
				return nodes.ToArray();
			}

			public DataAddress[] GetPathRootsSince(DataAddress root) {

				// The returned list,
				List<DataAddress> rootList = new List<DataAddress>(6);

				// Synchronize over the object
				lock (accessLock) {

					// Only allow if complete and synchronized
					CheckIsSynchronized();

					bool found = false;

					// Start position at the end of the file,
					long pos = internalFile.Length;

					while (found == false && pos > 0) {
						// Iterate backwards,
						pos = pos - RootItemSize;
						// Read the root node for this record,
						long rootNodeRefH = pagedFile.ReadInt64(pos + 8);
						long rootNodeRefL = pagedFile.ReadInt64(pos + 16);
						NodeId rootNodeId = new NodeId(rootNodeRefH, rootNodeRefL);
						// Crudely clear the cache if it's reached a certain threshold,
						pagedFile.ClearCache(4);
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

			internal void NotifyNewProposal(long uid, DataAddress proposal) {
				lock (accessLock) {
					if (!completeAndSynchronized) {
						// If this isn't complete, we add to the queue
						proposalQueue.Add(uid);
						proposalQueue.Add(proposal);
						return;
					}

					// Otherwise add the entry,
					PostProposalToPath(uid, proposal);
				}
			}

			public IPath Path {
				get {
					lock (accessLock) {
						if (pathFunction == null) {
							// Instantiate a new class object for the commit,
							Type c = Type.GetType(pathInfo.PathType, true, true);
							IPath processor = (IPath) Activator.CreateInstance(c, true);

							// Attach it with the PathAccess object,
							pathFunction = processor;
						}
						return pathFunction;
					}

				}
			}

			public string PathName {
				get { return pathName; }
			}
		}

		#endregion

		#region PathRecordEntry

		internal sealed class PathRecordEntry {
			public readonly long Uid;
			public readonly DataAddress Address;

			public PathRecordEntry(long uid, DataAddress address) {
				Uid = uid;
				Address = address;
			}
		}


		#endregion

		#region PathConnection

		private class PathConnection : IPathConnection {
			private readonly RootService service;
			private readonly PathInfo pathInfo;
			private readonly NetworkTreeSystem treeSystem;

			public PathConnection(RootService service, PathInfo pathInfo, IServiceConnector connector, IServiceAddress[] managerServers,
			                      INetworkCache cache, ServiceStatusTracker statusTracker) {
				this.service = service;
				this.pathInfo = pathInfo;
				treeSystem = new NetworkTreeSystem(connector, managerServers, cache, statusTracker);
			}

			private static ApplicationException HandleIOError(IOException e) {
				return new ApplicationException("IO Error: " + e.Message);
			}

			public DataAddress GetSnapshot() {
				try {
					return service.GetPathLast(pathInfo);
				} catch (IOException e) {
					throw HandleIOError(e);
				}
			}

			public DataAddress[] GetSnapshots(DateTime start, DateTime end) {
				try {
					return service.GetHistoricalPathRoots(pathInfo, DateTimeUtil.GetMillis(start), DateTimeUtil.GetMillis(end));
				} catch (IOException e) {
					throw HandleIOError(e);
				}
			}

			public DataAddress[] GetSnapshots(DataAddress rootNode) {
				try {
					return service.GetPathRootsSince(pathInfo, rootNode);
				} catch (IOException e) {
					throw HandleIOError(e);
				}
			}

			public void Publish(DataAddress rootNode) {
				try {
					service.PostToPath(pathInfo, rootNode);
				} catch (IOException e) {
					throw HandleIOError(e);
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
	}
}