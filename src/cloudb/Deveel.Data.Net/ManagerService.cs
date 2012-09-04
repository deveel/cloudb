using System;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public abstract class ManagerService : Service {
		private readonly IServiceAddress address;
		private readonly IServiceConnector connector;

		private IDatabase blockDatabase;
		private readonly object blockDbWriteLock = new object();

		private ReplicatedValueStore managerDb;

		private int managerUniqueId = -1;

		private readonly Dictionary<long, BlockServiceInfo> blockServersMap;
		private readonly List<BlockServiceInfo> blockServersList;
		private readonly List<RootServiceInfo> rootServersList;
		private readonly List<ManagerServiceInfo> managerServersList;

		private readonly ServiceStatusTracker serviceTracker;

		private DataAddress currentAddressSpaceEnd;
		private volatile bool freshAllocation = false;

		private long[] currentBlockIdServers;
		private readonly object allocationLock = new Object();

		private readonly BlockUpdateTask blockUpdateTask;

		private readonly Random rng;

		protected ManagerService(IServiceConnector connector, IServiceAddress address) {
			this.connector = connector;
			this.address = address;

			blockServersMap = new Dictionary<long, BlockServiceInfo>();
			blockServersList = new List<BlockServiceInfo>();
			rootServersList = new List<RootServiceInfo>();
			managerServersList = new List<ManagerServiceInfo>();

			rng = new Random();

			serviceTracker = new ServiceStatusTracker(connector);

			blockUpdateTask = new BlockUpdateTask(this);
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Manager; }
		}

		protected void SetBlockDatabase(IDatabase database) {
			this.blockDatabase = database;
			// Set the manager database,
			managerDb = new ReplicatedValueStore(address, connector, blockDatabase, blockDbWriteLock, serviceTracker);
		}

		protected override IMessageProcessor CreateProcessor() {
			return new MessageProcessor(this);
		}

		protected override void OnStart() {
			// Create a list of manager addresses,
			IServiceAddress[] managers;
			lock (managerServersList) {
				int sz = managerServersList.Count;
				managers = new IServiceAddress[sz];
				for (int i = 0; i < sz; ++i) {
					managers[i] = managerServersList[i].Address;
				}
			}

			// Perform the initialization
			managerDb.Initialize();

			// Set the task where every 5 minutes we update a block service
			new Timer(blockUpdateTask.Execute, null,
			          rng.Next(8*1000) + (15*1000),
			          rng.Next(30*1000) + (5*60*1000));
		}

		protected int UniqueManagerId {
			get {
				if (managerUniqueId == -1)
					throw new ApplicationException("This manager has not been registered to the network");

				return (managerUniqueId & 0x0FF);
			}
			set {
				if (managerUniqueId != -1)
					throw new ApplicationException("Unique id already set");
				managerUniqueId = value;
			}
		}

		private BlockId GetCurrentBlockIdAlloc() {
			lock (allocationLock) {
				if (currentAddressSpaceEnd == null) {
					// Ask the manager cluster for the last block id
					BlockId blockId = managerDb.GetLastBlockId();
					if (blockId == null) {
						// Initial state when the server map is empty,
						long nl = (256L & -256 /*0x0FFFFFFFFFFFFFF00L*/);
						nl += UniqueManagerId;
						blockId = new BlockId(0, nl);
					} else {
						blockId = blockId.Add(1024);
						// Clear the lower 8 bits and put the manager unique id there
						long nl = (blockId.Low & -256 /*0x0FFFFFFFFFFFFFF00L*/);
						nl += UniqueManagerId;
						blockId = new BlockId(blockId.High, nl);
					}

					return blockId;
				} else {
					return currentAddressSpaceEnd.BlockId;
				}
			}
		}

		private void InitCurrentAddressSpaceEnd() {
			lock (allocationLock) {
				if (currentAddressSpaceEnd == null) {
					// Gets the current block id being allocated against in this manager,
					BlockId blockId = GetCurrentBlockIdAlloc();

					// Set the current address end,
					currentBlockIdServers = AllocateNewBlock(blockId);
					currentAddressSpaceEnd = new DataAddress(blockId, 0);
				}
			}
		}

		protected void AddRegisteredBlockService(long serverGuid, IServiceAddress addr) {
			lock (blockServersMap) {
				BlockServiceInfo blockServer = new BlockServiceInfo(serverGuid, addr);

				// Add to the internal map/list
				blockServersMap[serverGuid] = blockServer;
				blockServersList.Add(blockServer);
			}
		}

		protected void AddRegisteredRootService(IServiceAddress addr) {
			lock (rootServersList) {
				// Add to the internal map/list
				rootServersList.Add(new RootServiceInfo(addr));
			}
		}

		protected void AddRegisteredManagerService(IServiceAddress addr) {
			lock (managerServersList) {
				// Add to the internal map/list
				managerServersList.Add(new ManagerServiceInfo(addr));

				// Add to the manager database
				managerDb.AddMachine(addr);
			}
		}

		private BlockServiceInfo[] GetServersInfo(long[] serversGuid) {
			List<BlockServiceInfo> reply;
			lock (blockServersMap) {
				int sz = serversGuid.Length;
				reply = new List<BlockServiceInfo>(sz);
				for (int i = 0; i < sz; ++i) {
					BlockServiceInfo blockServer;
					if (blockServersMap.TryGetValue(serversGuid[i], out blockServer)) {
						// Copy the server information into a new object.
						BlockServiceInfo nbs = new BlockServiceInfo(blockServer.ServerGuid, blockServer.Address);
						reply.Add(nbs);
					}
				}
			}
			return reply.ToArray();
		}

		protected abstract void PersistBlockServers(IList<BlockServiceInfo> serviceList);

		protected abstract void PersistRootServers(IList<RootServiceInfo> serviceList);

		protected abstract void PersistManagerServers(IList<ManagerServiceInfo> serversList);

		protected abstract void PersistManagerUniqueId(int uniqueId);

		private void InformRootServerOfManagers(IServiceAddress rootServer) {
			// Make the managers list
			List<IServiceAddress> managers = new List<IServiceAddress>(64);
			lock (managerServersList) {
				foreach (ManagerServiceInfo m in managerServersList) {
					managers.Add(m.Address);
				}
			}

			//TODO: verfy this ...
			// add the current manager address to the list
			managers.Add(address);

			IServiceAddress[] managersSet = managers.ToArray();

			Message message = new Message("informOfManagers", new object[] {managersSet});

			// Open a connection to the root server,
			IMessageProcessor processor = connector.Connect(rootServer, ServiceType.Root);
			IEnumerable<Message> response = processor.Process(message.AsStream());
			foreach (Message m in response) {
				if (m.HasError) {
					// If we failed, log a severe error but don't stop trying to register
					Logger.Error("Couldn't inform root server of managers");
					Logger.Error(m.ErrorStackTrace);

					if (ReplicatedValueStore.IsConnectionFault(m)) {
						serviceTracker.ReportServiceDownClientReport(rootServer, ServiceType.Root);
					}
				}
			}
		}

		private void ClearRootServerOfManagers(IServiceAddress rootServer) {
			Message message = new Message("clearOfManagers");

			// Open a connection to the root server,
			IMessageProcessor processor = connector.Connect(rootServer, ServiceType.Root);
			IEnumerable<Message> response = processor.Process(message.AsStream());
			foreach (Message m in response) {
				if (m.HasError) {
					// If we failed, log a severe error but don't stop trying to register
					Logger.Error("Couldn't inform root server of managers");
					Logger.Error(m.ErrorStackTrace);

					if (ReplicatedValueStore.IsConnectionFault(m)) {
						serviceTracker.ReportServiceDownClientReport(rootServer, ServiceType.Root);
					}
				}
			}
		}

		private void RegisterManagerServers(IServiceAddress[] managerServerAddresses) {
			// Sanity check on number of manager servers (10 should be enough for
			// everyone !)
			if (managerServerAddresses.Length > 100)
				throw new ApplicationException("Number of manager servers > 100");

			// Query all the manager servers on the network and generate a unique id
			// for this manager, if we need to create a new unique id,

			if (managerUniqueId == -1) {
				int sz = managerServerAddresses.Length;
				List<int> blacklistId = new List<int>(sz);
				for (int i = 0; i < sz; ++i) {
					IServiceAddress man = managerServerAddresses[i];
					if (!man.Equals(address)) {
						// Open a connection with the manager server,
						IMessageProcessor processor = connector.Connect(man, ServiceType.Manager);

						// Query the unique id of the manager server,
						Message message = new Message("getUniqueId");
						IEnumerable<Message> response = processor.Process(message.AsStream());
						foreach (Message m in response) {
							if (m.HasError)
								throw new ApplicationException(m.ErrorMessage);

							long uniqueId = (long) m.Arguments[0].Value;
							if (uniqueId == -1)
								throw new ApplicationException("getUniqueId = -1");

							// Add this to blacklist,
							blacklistId.Add((int) uniqueId);
						}
					}
				}

				// Find a random id not found in the blacklist,
				int genId;
				while (true) {
					genId = rng.Next(200);
					if (!blacklistId.Contains(genId)) {
						break;
					}
				}

				// Set the unique id,
				managerUniqueId = genId;
			}

			lock (managerServersList) {
				managerServersList.Clear();
				managerDb.ClearAllMachines();

				foreach (IServiceAddress m in managerServerAddresses) {
					if (!m.Equals(address)) {
						ManagerServiceInfo managerServer = new ManagerServiceInfo(m);
						managerServersList.Add(managerServer);
						// Add to the manager database
						managerDb.AddMachine(managerServer.Address);
					}
				}

				PersistManagerServers(managerServersList);
				PersistManagerUniqueId(managerUniqueId);
			}

			// Perform initialization on the manager
			managerDb.Initialize();

			// Wait for initialization to complete,
			managerDb.WaitInitComplete();

			// Add a manager server entry,
			foreach (IServiceAddress managerAddr in managerServerAddresses) {
				managerDb.SetValue("ms." + managerAddr, "");
			}

			// Tell all the root servers of the new manager set,
			List<IServiceAddress> rootServersSet = new List<IServiceAddress>(64);
			lock (rootServersList) {
				foreach (RootServiceInfo rs in rootServersList) {
					rootServersSet.Add(rs.Address);
				}
			}

			foreach (IServiceAddress r in rootServersSet) {
				InformRootServerOfManagers(r);
			}
		}

		private void DeregisterManagerServer(IServiceAddress managerServerAddress) {
			// Create a list of servers to be deregistered,
			List<ManagerServiceInfo> toRemove;
			lock (managerServersList) {
				toRemove = new List<ManagerServiceInfo>(32);
				foreach (ManagerServiceInfo item in managerServersList) {
					if (item.Address.Equals(managerServerAddress)) {
						toRemove.Add(item);
					}
				}
			}

			// Remove the entries and persist
			lock (managerServersList) {
				// Remove the entries that match,
				foreach (ManagerServiceInfo item in toRemove) {
					managerServersList.Remove(item);
					// Add to the manager database
					managerDb.RemoveMachine(item.Address);
				}

				PersistManagerServers(managerServersList);

				// Clear the unique id if we are deregistering this service,
				if (managerServerAddress.Equals(address)) {
					managerUniqueId = -1;
					PersistManagerUniqueId(managerUniqueId);
				}
			}

			// Perform initialization on the manager
			managerDb.Initialize();

			// Wait for initialization to complete,
			managerDb.WaitInitComplete();

			// Remove the manager server entry,
			managerDb.SetValue("ms." + managerServerAddress, null);

			// Tell all the root servers of the new manager set,
			List<IServiceAddress> rootServersSet = new List<IServiceAddress>(64);
			lock (rootServersList) {
				foreach (RootServiceInfo rs in rootServersList) {
					rootServersSet.Add(rs.Address);
				}
			}

			foreach (IServiceAddress r in rootServersSet) {
				InformRootServerOfManagers(r);
			}
		}

		private void RegisterRootServer(IServiceAddress rootServerAddress) {
			// The root server object,
			RootServiceInfo rootServer = new RootServiceInfo(rootServerAddress);

			// Add it to the map
			lock (rootServersList) {
				rootServersList.Add(rootServer);
				PersistRootServers(rootServersList);
			}

			// Add the root server entry,
			managerDb.SetValue("rs." + rootServerAddress, "");

			// Tell root server about the managers.
			InformRootServerOfManagers(rootServerAddress);
		}

		private void DeregisterRootServer(IServiceAddress rootServerAddress) {
			// Remove it from the map and persist
			lock (rootServersList) {
				// Find the server to remove,
				for (int i = rootServersList.Count - 1; i >= 0; i--) {
					RootServiceInfo server = rootServersList[i];
					if (server.Address.Equals(rootServerAddress)) {
						rootServersList.RemoveAt(i);
					}
				}

				PersistRootServers(rootServersList);
			}

			// Remove the root server entry,
			managerDb.SetValue("rs." + rootServerAddress, null);

			// Tell root server about the managers.
			ClearRootServerOfManagers(rootServerAddress);
		}

		private void DeregisterAllRootServers() {
			// Create a list of servers to be deregistered,
			List<RootServiceInfo> toRemove;
			lock (rootServersList) {
				toRemove = new List<RootServiceInfo>(rootServersList.Count);
				toRemove.AddRange(rootServersList);
			}

			// Remove the entries from the map and persist
			lock (rootServersList) {
				// Remove the entries that match,
				foreach (RootServiceInfo item in toRemove) {
					rootServersList.Remove(item);
				}

				PersistRootServers(rootServersList);
			}

			// Clear the managers set from all the root servers,
			foreach (RootServiceInfo item in toRemove) {
				ClearRootServerOfManagers(item.Address);
			}
		}

		private void RegisterBlockServer(IServiceAddress blockServerAddress) {

			// Get the block server uid,
			Message message = new Message("serverGUID");

			// Connect to the block server,
			IMessageProcessor processor = connector.Connect(blockServerAddress, ServiceType.Block);
			IEnumerable<Message> response = processor.Process(message.AsStream());
			Message rm = null;
			foreach (Message m in response) {
				if (m.HasError)
					throw new ApplicationException(m.ErrorMessage);

				rm = m;
			}

			long serverGuid = (long) rm.Arguments[0].Value;

			// Add lookup for this server_guid <-> service address to the db,
			managerDb.SetValue("block.sguid." + serverGuid, blockServerAddress.ToString());
			managerDb.SetValue("block.addr." + blockServerAddress, serverGuid.ToString());

			// TODO: Block discovery on the introduced machine,


			// Set the status and guid
			BlockServiceInfo blockServer = new BlockServiceInfo(serverGuid, blockServerAddress);
			// Add it to the map
			lock (blockServersMap) {
				blockServersMap[serverGuid] = blockServer;
				blockServersList.Add(blockServer);
				PersistBlockServers(blockServersList);
			}
		}

		private void DeregisterBlockServer(IServiceAddress blockServerAddress) {
			// Remove from the db,
			string blockAddrKey = "block.addr." + blockServerAddress;
			string serverSguidStr = managerDb.GetValue(blockAddrKey);
			if (serverSguidStr != null) {
				managerDb.SetValue("block.sguid." + serverSguidStr, null);
				managerDb.SetValue(blockAddrKey, null);
			}

			// Remove it from the map and persist
			lock (blockServersMap) {
				// Find the server to remove,
				List<BlockServiceInfo> toRemove = new List<BlockServiceInfo>();
				foreach (BlockServiceInfo server in blockServersList) {
					if (server.Address.Equals(blockServerAddress)) {
						toRemove.Add(server);
					}
				}
				// Remove the entries that match,
				foreach (BlockServiceInfo item in toRemove) {
					blockServersMap.Remove(item.ServerGuid);
					blockServersList.Remove(item);
				}

				PersistBlockServers(blockServersList);
			}

			// Check that we aren't allocating against servers no longer in
			// the list. If so, fix the error.
			CheckAndFixAllocationServers();
		}

		private void DeregisterAllBlockServers() {
			// Create a list of servers to be deregistered,
			List<BlockServiceInfo> toRemove;
			lock (blockServersMap) {
				toRemove = new List<BlockServiceInfo>(blockServersList.Count);
				toRemove.AddRange(blockServersList);
			}

			// Remove all items in the to_remove from the db,
			foreach (BlockServiceInfo item in toRemove) {
				IServiceAddress blockServerAddress = item.Address;
				string blockAddrKey = "block.addr." + blockServerAddress;
				string serverSguidStr = managerDb.GetValue(blockAddrKey);
				if (serverSguidStr != null) {
					managerDb.SetValue("block.sguid." + serverSguidStr, null);
					managerDb.SetValue(blockAddrKey, null);
				}
			}

			// Remove the entries from the map and persist
			lock (blockServersMap) {
				// Remove the entries that match,
				foreach (BlockServiceInfo item in toRemove) {
					blockServersMap.Remove(item.ServerGuid);
					blockServersList.Remove(item);
				}

				PersistBlockServers(blockServersList);
			}

			// Check that we aren't allocating against servers no longer in
			// the list. If so, fix the error.
			CheckAndFixAllocationServers();
		}

		private String[] GetAllPaths() {
			const string prefix = "path.info.";

			// Get all the keys with prefix 'path.info.'
			string[] pathKeys = managerDb.GetKeys(prefix);

			// Remove the prefix
			for (int i = 0; i < pathKeys.Length; ++i) {
				pathKeys[i] = pathKeys[i].Substring(prefix.Length);
			}

			return pathKeys;
		}

		private PathInfo GetPathInfoForPath(string pathName) {
			string pathInfoContent = managerDb.GetValue("path.info." + pathName);

			if (pathInfoContent == null)
				return null;

			// Create and return the path info object,
			return PathInfo.Parse(pathName, pathInfoContent);
		}

		private void AddPathToNetwork(string pathName, string pathType, IServiceAddress rootLeader,
		                              IServiceAddress[] rootServers) {
			if (pathType.Contains("|"))
				throw new ArgumentException("Invalid path type string", "pathType");
			if (pathName.Contains("|"))
				throw new ArgumentException("Invalid path name string", "pathName");

			string key = "path.info." + pathName;
			// Check the map doesn't already exist
			if (managerDb.GetValue(key) != null)
				throw new ApplicationException("Path already assigned");

			// Set the first path info version for this path name
			PathInfo mpathInfo = new PathInfo(pathName, pathType, 1, rootLeader, rootServers);

			// Add the path to the manager db cluster.
			managerDb.SetValue(key, mpathInfo.ToString());
		}

		private void RemovePathFromNetwork(string pathName) {
			string key = "path.info." + pathName;

			// Remove the path from the manager db cluster,
			managerDb.SetValue(key, null);
		}

		private long[] AllocateOnlineServerNodesForBlock(BlockId blockId) {
			// Fetch the list of all online servers,
			List<BlockServiceInfo> servSet;
			lock (blockServersMap) {
				servSet = new List<BlockServiceInfo>(blockServersList.Count);
				foreach (BlockServiceInfo server in blockServersList) {
					// Add the servers with status 'up'
					if (serviceTracker.IsServiceUp(server.Address, ServiceType.Block)) {
						servSet.Add(server);
					}
				}
			}

			// TODO: This is a simple random server picking method for a block.
			//   We should prioritize servers picked based on machine specs, etc.

			int sz = servSet.Count;
			// If serv_set is 3 or less, we return the servers available,
			if (sz <= 3) {
				long[] returnVal = new long[sz];
				for (int i = 0; i < sz; ++i) {
					BlockServiceInfo blockServer = servSet[i];
					returnVal[i] = blockServer.ServerGuid;
				}
				return returnVal;
			} else {
				// Randomly pick three servers from the list,
				long[] returnVal = new long[3];
				for (int i = 0; i < 3; ++i) {
					// java.util.Random is specced to be thread-safe,
					int randomI = rng.Next(servSet.Count);
					BlockServiceInfo blockServer = servSet[i];
					servSet.RemoveAt(randomI);
					returnVal[i] = blockServer.ServerGuid;
				}

				// Return the array,
				return returnVal;
			}
		}

		private void NotifyBlockServerOfMaxBlockId(IServiceAddress blockServer, BlockId blockId) {
			if (serviceTracker.IsServiceUp(blockServer, ServiceType.Block)) {
				Message message = new Message("notifyCurrentBlockId", blockId);
				// Connect to the block server,
				IMessageProcessor processor = connector.Connect(blockServer, ServiceType.Block);
				IEnumerable<Message> response = processor.Process(message.AsStream());
				// If the block server is down, report it to the tracker,
				foreach (Message m in response) {
					if (m.HasError) {
						if (ReplicatedValueStore.IsConnectionFault(m)) {
							serviceTracker.ReportServiceDownClientReport(blockServer, ServiceType.Block);
						}
					}
				}
			}
		}

		private void NotifyBlockServersOfCurrentBlockId(long[] blockServersNotify, BlockId blockId) {
			// Copy the block servers list for concurrency safety,
			List<BlockServiceInfo> blockServersListCopy = new List<BlockServiceInfo>(64);
			lock (blockServersMap) {
				blockServersListCopy.AddRange(blockServersList);
			}

			// For each block server
			foreach (BlockServiceInfo blockServer in blockServersListCopy) {
				// Is it in the block_servers_notify list?
				bool found = false;
				foreach (long bsn in blockServersNotify) {
					if (blockServer.ServerGuid == bsn) {
						found = true;
						break;
					}
				}

				// If found and the service is up,
				if (found) {
					NotifyBlockServerOfMaxBlockId(blockServer.Address, blockId);
				}
			}
		}

		private class NewBlockAllocInfo {
			public readonly long[] BlockServersToNotify;
			public readonly BlockId BlockId;

			public NewBlockAllocInfo(BlockId blockId, long[] blockServersToNotify) {
				BlockId = blockId;
				BlockServersToNotify = blockServersToNotify;
			}
		}

		private void NewBlockAllocTask(object state) {
			NewBlockAllocInfo info = (NewBlockAllocInfo) state;
			NotifyBlockServersOfCurrentBlockId(info.BlockServersToNotify, info.BlockId);
		}

		private long[] AllocateNewBlock(BlockId blockId) {

			lock (allocationLock) {

				// Schedule a task that informs all the current block servers what the
				// new block being allocated against is. This notifies them that they
				// can perform maintenance on all blocks preceding, such as compression.
				long[] blockServersNotify = currentBlockIdServers;
				if (blockServersNotify != null) {
					NewBlockAllocInfo info = new NewBlockAllocInfo(blockId, blockServersNotify);
					new Timer(NewBlockAllocTask, info, 500, Timeout.Infinite);
				}

				// Assert the block isn't already allocated,
				long[] currentServers = managerDb.GetBlockIdServerMap(blockId);
				if (currentServers.Length > 0) {
					throw new NetworkWriteException("Block already allocated: " + blockId);
				}

				// Allocate a group of servers from the poll of block servers for the
				// given block_id
				long[] servers = AllocateOnlineServerNodesForBlock(blockId);

				// If no servers allocated,
				if (servers.Length == 0) {
					throw new ApplicationException("Unable to assign block servesr for block: " + blockId);
				}

				// Update the database,
				managerDb.SetBlockIdServerMap(blockId, servers);

				// Return the list,
				return servers;
			}
		}

		private long[] GetOnlineServersWithBlock(BlockId blockId) {
			// Fetch the server map for the block from the db cluster,
			return managerDb.GetBlockIdServerMap(blockId);
		}

		private void InternalAddBlockServerMapping(BlockId blockId, long[] serverGuids) {
			long[] currentServerGuids = managerDb.GetBlockIdServerMap(blockId);
			List<long> serverList = new List<long>(64);
			foreach (long s in currentServerGuids) {
				serverList.Add(s);
			}

			// Add the servers to the list,
			foreach (long s in serverGuids) {
				if (!serverList.Contains(s)) {
					serverList.Add(s);
				}
			}

			// Set the new list
			long[] newServerGuids = new long[serverList.Count];
			for (int i = 0; i < serverList.Count; ++i) {
				newServerGuids[i] = serverList[i];
			}

			managerDb.SetBlockIdServerMap(blockId, newServerGuids);
		}

		private void InternalRemoveBlockServerMapping(BlockId blockId, long[] serverGuids) {
			long[] currentServerGuids = managerDb.GetBlockIdServerMap(blockId);
			List<long> serverList = new List<long>(64);
			foreach (long s in currentServerGuids) {
				serverList.Add(s);
			}

			// Remove the servers from the list
			foreach (long s in serverGuids) {
				int index = serverList.IndexOf(s);
				if (index >= 0) {
					serverList.RemoveAt(index);
				}
			}

			// Set the new list
			long[] newServerGuids = new long[serverList.Count];
			for (int i = 0; i < serverList.Count; ++i) {
				newServerGuids[i] = serverList[i];
			}

			managerDb.SetBlockIdServerMap(blockId, newServerGuids);
		}

		private void CheckAndFixAllocationServers() {
			// If the failure report is on a block server that is servicing allocation
			// requests, we push the allocation requests to the next block.
			BlockId currentBlockId;
			lock (allocationLock) {
				// Check address_space_end is initialized
				InitCurrentAddressSpaceEnd();
				currentBlockId = currentAddressSpaceEnd.BlockId;
			}
			long[] bservers = GetOnlineServersWithBlock(currentBlockId);
			int okServerCount = 0;

			lock (blockServersMap) {
				// For each server that stores the block,
				for (int i = 0; i < bservers.Length; ++i) {
					long serverGuid = bservers[i];
					// Is the status of this server UP?
					foreach (BlockServiceInfo blockServer in blockServersList) {
						// If this matches the guid, and is up, we add to 'ok_server_count'
						if (blockServer.ServerGuid == serverGuid &&
						    serviceTracker.IsServiceUp(blockServer.Address, ServiceType.Block)) {
							++okServerCount;
						}
					}
				}
			}

			// If the count of ok servers for the allocation set size is not
			// the same then there are one or more servers that are inoperable
			// in the allocation set. So, we increment the block id ref of
			// 'current_address_space_end' by 1 to force a reevaluation of the
			// servers to allocate the current block.
			if (okServerCount != bservers.Length) {
				Logger.Info("Moving current_address_space_end past unavailable block");

				bool nextBlock = false;
				lock (allocationLock) {
					BlockId blockId = currentAddressSpaceEnd.BlockId;
					int data_id = currentAddressSpaceEnd.DataId;
					DataAddress newAddressSpaceEnd = null;
					if (currentBlockId.Equals(blockId)) {
						blockId = blockId.Add(256);
						data_id = 0;
						newAddressSpaceEnd = new DataAddress(blockId, data_id);
						nextBlock = true;
					}

					// Allocate a new block (happens under 'allocation_lock')
					if (nextBlock) {
						currentBlockIdServers = AllocateNewBlock(blockId);
						currentAddressSpaceEnd = newAddressSpaceEnd;
					}
				}
			}
		}

		private void NotifyBlockServerFailure(IServiceAddress serverAddress) {
			// If the server currently recorded as up,
			if (serviceTracker.IsServiceUp(serverAddress, ServiceType.Block)) {
				// Report the block service down to the service tracker,
				serviceTracker.ReportServiceDownClientReport(serverAddress, ServiceType.Block);
			}

			// Change the allocation point if we are allocating against servers that
			// have failed,
			CheckAndFixAllocationServers();
		}

		private void NotifyBlockIdCorruption(IServiceAddress serverAddress, BlockId blockId, string failureType) {
			// TODO:
		}

		#region MessageProcessor

		private class MessageProcessor : IMessageProcessor {
			private readonly ManagerService service;

			public MessageProcessor(ManagerService service) {
				this.service = service;
			}

			public IEnumerable<Message> Process(IEnumerable<Message> stream) {
				// The reply message,
				MessageStream replyMessage = new MessageStream();

				// The messages in the stream,
				foreach (Message m in stream) {
					try {
						// Check the server isn't in a stop state,
						service.CheckErrorState();
						String cmd = m.Name;

						// getServerList(BlockId)
						if (cmd.Equals("getServerList")) {
							BlockServiceInfo[] servers = GetServerList((BlockId) m.Arguments[0].Value);
							Message response = new Message();
							response.Arguments.Add(servers.Length);
							for (int i = 0; i < servers.Length; ++i) {
								response.Arguments.Add(servers[i].Address);
								response.Arguments.Add(service.serviceTracker.GetServiceCurrentStatus(servers[i].Address, ServiceType.Block));
							}

							replyMessage.AddMessage(response);
						}
							// allocateNode(int node_size)
						else if (cmd.Equals("allocateNode")) {
							DataAddress address = AllocateNode((int) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(address));
						}
							// registerBlockServer(ServiceAddress service_address)
						else if (cmd.Equals("registerBlockServer")) {
							service.RegisterBlockServer((IServiceAddress) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// deregisterBlockServer(IServiceAddress)
						else if (cmd.Equals("deregisterBlockServer")) {
							service.DeregisterBlockServer((IServiceAddress) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// deregisterAllBlockServers()
						else if (cmd.Equals("deregisterAllBlockServers")) {
							service.DeregisterAllBlockServers();
							replyMessage.AddMessage(new Message(1));
						}

							// registerManagerServers(ServiceAddress[] managers)
						else if (cmd.Equals("registerManagerServers")) {
							service.RegisterManagerServers((IServiceAddress[]) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// deregisterAllManagerServers()
						else if (cmd.Equals("deregisterManagerServer")) {
							service.DeregisterManagerServer((IServiceAddress) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// addPathToNetwork(string, string, IServiceAddress, IServiceAddress[])
						else if (cmd.Equals("addPathToNetwork")) {
							service.AddPathToNetwork((string) m.Arguments[0].Value, (string) m.Arguments[1].Value,
							                         (IServiceAddress) m.Arguments[2].Value, (IServiceAddress[]) m.Arguments[3].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// removePathFromNetwork(String path_name)
						else if (cmd.Equals("removePathFromNetwork")) {
							service.RemovePathFromNetwork((string) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}

							// addBlockServerMapping(BlockId, long[])
						else if (cmd.Equals("internalAddBlockServerMapping")) {
							service.InternalAddBlockServerMapping((BlockId) m.Arguments[0].Value, (long[]) m.Arguments[1].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// removeBlockServerMapping(BlockId, long[])
						else if (cmd.Equals("internalRemoveBlockServerMapping")) {
							service.InternalRemoveBlockServerMapping((BlockId) m.Arguments[0].Value, (long[]) m.Arguments[1].Value);
							replyMessage.AddMessage(new Message(1));
						}

							// --- Path processors ---

							// registerRootServer(IServiceAddress)
						else if (cmd.Equals("registerRootServer")) {
							service.RegisterRootServer((IServiceAddress) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// deregisterRootServer(IServiceAddress)
						else if (cmd.Equals("deregisterRootServer")) {
							service.DeregisterRootServer((IServiceAddress) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}
							// deregisterAllConsensusProcessors()
						else if (cmd.Equals("deregisterAllRootServers")) {
							service.DeregisterAllRootServers();
							replyMessage.AddMessage(new Message(1));
						}

							// PathInfo getPathInfoForPath(string)
						else if (cmd.Equals("getPathInfoForPath")) {
							PathInfo pathInfo = service.GetPathInfoForPath((String) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(pathInfo));
						}
							// string[] getAllPaths()
						else if (cmd.Equals("getAllPaths")) {
							string[] pathSet = service.GetAllPaths();
							replyMessage.AddMessage(new Message(new object[] {pathSet}));
						}
							// getRegisteredServerList()
						else if (cmd.Equals("getRegisteredServerList")) {
							GetRegisteredServerList(replyMessage);
						}
							// getRegisteredBlockServers()
						else if (cmd.Equals("getRegisteredBlockServers")) {
							GetRegisteredBlockServers(replyMessage);
						}
							// getRegisteredRootServers()
						else if (cmd.Equals("getRegisteredRootServers")) {
							GetRegisteredRootServers(replyMessage);
						}

							// notifyBlockServerFailure(IServiceAddress)
						else if (cmd.Equals("notifyBlockServerFailure")) {
							service.NotifyBlockServerFailure((IServiceAddress) m.Arguments[0].Value);
							replyMessage.AddMessage(new Message(1));
						}

							// notifyBlockIdCorruption(IServiceAddress, BlockId, string)
						else if (cmd.Equals("notifyBlockIdCorruption")) {
							service.NotifyBlockIdCorruption((IServiceAddress) m.Arguments[0].Value, (BlockId) m.Arguments[1].Value,
							                                (string) m.Arguments[2].Value);
							replyMessage.AddMessage(new Message(1));
						}

							// getUniqueId()
						else if (cmd.Equals("getUniqueId")) {
							long uniqueId = service.managerUniqueId;
							replyMessage.AddMessage(new Message(uniqueId));
						}

							// poll(String)
						else if (m.Name.Equals("poll")) {
							service.managerDb.CheckConnected();
							replyMessage.AddMessage(new Message(1));
						} else {
							// Defer to the manager db process command,
							service.managerDb.Process(m, replyMessage);

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

			private void GetRegisteredServerList(MessageStream outputStream) {
				// Populate the list of registered servers
				IServiceAddress[] srvs;
				ServiceStatus[] statusCodes;
				lock (service.blockServersMap) {
					int sz = service.blockServersList.Count;
					srvs = new IServiceAddress[sz];
					statusCodes = new ServiceStatus[sz];
					int i = 0;
					foreach (BlockServiceInfo m in service.blockServersList) {
						srvs[i] = m.Address;
						statusCodes[i] = service.serviceTracker.GetServiceCurrentStatus(m.Address, ServiceType.Block);
						++i;
					}
				}

				// Populate the reply message,
				outputStream.AddMessage(new Message(srvs, statusCodes));
			}

			private void GetRegisteredBlockServers(MessageStream msg_out) {
				// Populate the list of registered block servers
				long[] guids;
				IServiceAddress[] srvs;
				lock (service.blockServersMap) {
					int sz = service.blockServersList.Count;
					guids = new long[sz];
					srvs = new IServiceAddress[sz];
					int i = 0;
					foreach (BlockServiceInfo m in service.blockServersList) {
						guids[i] = m.ServerGuid;
						srvs[i] = m.Address;
						++i;
					}
				}

				// The reply message,
				msg_out.AddMessage(new Message(guids, srvs));
			}

			private void GetRegisteredRootServers(MessageStream outputStream) {
				// Populate the list of registered root servers
				IServiceAddress[] srvs;
				lock (service.rootServersList) {
					int sz = service.rootServersList.Count;
					srvs = new IServiceAddress[sz];
					int i = 0;
					foreach (RootServiceInfo m in service.rootServersList) {
						srvs[i] = m.Address;
						++i;
					}
				}

				// The reply message,
				outputStream.AddMessage(new Message(new object[] {srvs}));
			}

			private BlockServiceInfo[] GetServerList(BlockId blockId) {

				// Query the local database for the server list of the block.  If the
				// block doesn't exist in the database then it provisions it over the
				// network.

				long[] serverIds = service.GetOnlineServersWithBlock(blockId);

				// Resolve the server ids into server names and parse it as a reply
				int sz = serverIds.Length;

				// No online servers contain the block
				if (sz == 0)
					throw new ApplicationException("No online servers for block: " + blockId);

				BlockServiceInfo[] reply = service.GetServersInfo(serverIds);

				service.Logger.Info(String.Format("getServersInfo replied {0} for {1}", reply.Length, blockId));

				return reply;
			}

			private DataAddress AllocateNode(int nodeSize) {
				if (nodeSize >= 65536) {
					throw new ArgumentException("node_size too large", "nodeSize");
				} else if (nodeSize < 0) {
					throw new ArgumentException("node_size too small", "nodeSize");
				}

				BlockId blockId;
				int data_id;
				bool next_block = false;
				BlockId next_block_id = null;

				lock (service.allocationLock) {

					// Check address_space_end is initialized
					service.InitCurrentAddressSpaceEnd();

					// Set fresh allocation to false because we allocated off the
					// current address space,
					service.freshAllocation = false;

					// Fetch the current block of the end of the address space,
					blockId = service.currentAddressSpaceEnd.BlockId;
					// Get the data identifier,
					data_id = service.currentAddressSpaceEnd.DataId;

					// The next position,
					int next_data_id = data_id;
					next_block_id = blockId;
					++next_data_id;
					if (next_data_id >= 16384) {
						next_data_id = 0;
						next_block_id = next_block_id.Add(256);
						next_block = true;
					}

					// Before we return this allocation, if we went to the next block we
					// sync the block allocation with the other managers.

					if (next_block) {
						service.currentBlockIdServers = service.AllocateNewBlock(next_block_id);
					}

					// Update the address space end,
					service.currentAddressSpaceEnd = new DataAddress(next_block_id, next_data_id);

				}

				// Return the data address,
				return new DataAddress(blockId, data_id);
			}
		}


		#endregion

		#region BlockServiceInfo

		protected class BlockServiceInfo {
			public readonly long ServerGuid;
			public readonly IServiceAddress Address;

			public BlockServiceInfo(long serverGuid, IServiceAddress address) {
				ServerGuid = serverGuid;
				Address = address;
			}
		}

		#endregion

		#region RootServiceInfo

		protected class RootServiceInfo {
			public readonly IServiceAddress Address;

			public RootServiceInfo(IServiceAddress address) {
				Address = address;
			}
		}

		#endregion

		#region ManagerServiceInfo

		protected class ManagerServiceInfo {
			public readonly IServiceAddress Address;

			public ManagerServiceInfo(IServiceAddress address) {
				Address = address;
			}
		}

		#endregion

		#region BlockUpdateTask

		private class BlockUpdateTask {
			private readonly ManagerService service;
			private bool init;
			private int blockIdIndex;
			private BlockId currentEndBlock;

			public BlockUpdateTask(ManagerService service) {
				this.service = service;
			}

			public void Execute(object state) {
				BlockServiceInfo blockToCheck;

				// Cycle through the block servers list,
				lock (service.blockServersMap) {
					if (service.blockServersList.Count == 0) {
						return;
					}
					if (init == false)
						blockIdIndex = service.rng.Next(service.blockServersList.Count);

					blockToCheck = service.blockServersList[blockIdIndex];
					++blockIdIndex;
					if (blockIdIndex >= service.blockServersList.Count) {
						blockIdIndex = 0;
					}
					init = true;
				}

				// Notify the block server of the current block,
				BlockId currentBlockId;
				lock (service.allocationLock) {
					if (service.currentAddressSpaceEnd == null) {
						if (currentEndBlock == null) {
							currentEndBlock = service.GetCurrentBlockIdAlloc();
						}
						currentBlockId = currentEndBlock;
					} else {
						currentBlockId = service.currentAddressSpaceEnd.BlockId;
					}
				}

				// Notify the block server we are cycling through of the maximum block id.
				service.NotifyBlockServerOfMaxBlockId(blockToCheck.Address, currentBlockId);
			}
		}

		#endregion
	}
}