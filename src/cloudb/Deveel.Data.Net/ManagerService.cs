using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public abstract class ManagerService : Service {
		private readonly IServiceConnector connector;
		private readonly IServiceAddress address;

		private DataAddress currentAddressSpaceEnd;
		private readonly object allocationLock = new object();

		private readonly Dictionary<long, BlockServerInfo> blockServersMap;
		private readonly List<BlockServerInfo> blockServers;
		private readonly List<RootServerInfo> rootServers;
		private readonly List<ManagerServerInfo> managerServers;
		private IDatabase blockDatabase;
		private readonly object blockDbWriteLock = new object();
		private readonly Random random;

		private ReplicatedValueStore managerDb;
		private int uniqueId = -1;

		private Timer timer;

		private readonly ServiceStatusTracker serviceTracker;

		private volatile bool fresh_allocation = false;
		private long[] currentBlockIdServers;

		private static readonly Key BlockServerKey = new Key(12, 0, 10);
		private static readonly Key PathRootKey = new Key(12, 0, 20);

		protected ManagerService(IServiceConnector connector, IServiceAddress address) {
			this.connector = connector;
			this.address = address;

			blockServersMap = new Dictionary<long, BlockServerInfo>(256);
			blockServers = new List<BlockServerInfo>(256);
			rootServers = new List<RootServerInfo>(256);
			managerServers = new List<ManagerServerInfo>(256);
			random = new Random();

			serviceTracker = new ServiceStatusTracker(connector);
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Manager; }
		}

		protected override IMessageProcessor CreateProcessor() {
			return new ManagerServerMessageProcessor(this);
		}

		protected int UniqueId {
			get {
				if (uniqueId == -1)
					throw new ApplicationException("This manager has not been registered to the network");

				return (uniqueId & 0x0FF);
			}
			set {
				if (uniqueId != -1)
					throw new ApplicationException("Unique id already set");
				uniqueId = value;
			}
		}

		private BlockId GetCurrentBlockIdAlloc() {
			lock (allocationLock) {
				if (currentAddressSpaceEnd == null) {
					// Ask the manager cluster for the last block id
					BlockId blockId = managerDb.GetLastBlockId();
					if (blockId == null) {
						// Initial state when the server map is empty,
						long nl = (long) (256L & 0x0FFFFFFFFFFFFFF00L);
						nl += UniqueId;
						blockId = new BlockId(0, nl);
					} else {
						blockId = blockId.Add(1024);
						// Clear the lower 8 bits and put the manager unique id there
						long nl = (long) ((ulong) blockId.Low & 0x0FFFFFFFFFFFFFF00L);
						nl += UniqueId;
						blockId = new BlockId(blockId.High, nl);
					}

					return blockId;
				}
				
				return currentAddressSpaceEnd.BlockId;
			}
		}

		private void InitCurrentAddressSpaceEnd() {
			lock (allocationLock) {
				if (currentAddressSpaceEnd == null) {

					// Gets the current block id being allocated against in this manager,
					BlockId block_id = GetCurrentBlockIdAlloc();

					// Set the current address end,
					currentBlockIdServers = AllocateNewBlock(block_id);
					currentAddressSpaceEnd = new DataAddress(block_id, 0);
				}
			}
		}

		private Timer notifyTimer;

		private void NotifyTask(object state) {
			BlockId blockId = (BlockId) state;
			NotifyBlockServersOfCurrentBlockId(currentBlockIdServers, blockId);
		}

		private long[] AllocateNewBlock(BlockId blockId) {
			lock (allocationLock) {
				// Schedule a task that informs all the current block servers what the
				// new block being allocated against is. This notifies them that they
				// can perform maintenance on all blocks preceding, such as compression.
				long[] block_servers_notify = currentBlockIdServers;
				if (block_servers_notify != null) {
					notifyTimer = new Timer(NotifyTask, blockId, 500, Timeout.Infinite);
				}

				// Assert the block isn't already allocated,
				long[] current_servers = managerDb.GetBlockIdServerMap(blockId);
				if (current_servers.Length > 0) {
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

		private long[] AllocateOnlineServerNodesForBlock(BlockId blockId) {
			// Fetch the list of all online servers,
			List<BlockServerInfo> servSet;
			lock (blockServersMap) {
				servSet = new List<BlockServerInfo>(blockServers.Count);
				foreach (BlockServerInfo server in blockServers) {
					// Add the servers with status 'up'
					if (serviceTracker.IsServiceUp(server.Address, ServiceType.Block))
						servSet.Add(server);
				}
			}

			// TODO: This is a simple random server picking method for a block.
			//   We should prioritize servers picked based on machine specs, etc.

			long[] returnVal;
			int sz = servSet.Count;
			// If serv_set is 3 or less, we return the servers available,
			if (sz <= 3) {
				returnVal = new long[sz];
				for (int i = 0; i < sz; ++i) {
					BlockServerInfo blockServer = servSet[i];
					returnVal[i] = blockServer.Guid;
				}
				return returnVal;
			}

			// Randomly pick three servers from the list,
			returnVal = new long[3];
			for (int i = 0; i < 3; ++i) {
				// java.util.Random is specced to be thread-safe,
				int randomI = random.Next(servSet.Count);
				BlockServerInfo blockServer = servSet[randomI];
				servSet.RemoveAt(randomI);
				returnVal[i] = blockServer.Guid;
			}

			// Return the array,
			return returnVal;
		}

		private long[] GetOnlineServersWithBlock(BlockId blockId) {
			// Fetch the server map for the block from the db cluster,
			return managerDb.GetBlockIdServerMap(blockId);
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
					foreach (BlockServerInfo blockServer in blockServers) {
						// If this matches the guid, and is up, we add to 'ok_server_count'
						if (blockServer.Guid == serverGuid &&
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
				BlockId blockId;
				lock (allocationLock) {
					blockId = currentAddressSpaceEnd.BlockId;
					int dataId = currentAddressSpaceEnd.DataId;
					DataAddress newAddressSpaceEnd = null;
					if (currentBlockId.Equals(blockId)) {
						blockId = blockId.Add(256);
						dataId = 0;
						newAddressSpaceEnd = new DataAddress(blockId, dataId);
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

		private void ClearRootServerOfManagers(IServiceAddress rootService) {
			RequestMessage request = new RequestMessage("clearOfManagers");

			// Open a connection to the root server,
			IMessageProcessor processor = connector.Connect(rootService, ServiceType.Root);
			Message response = processor.Process(request);
			if (response.HasError) {
				// If we failed, log a severe error but don't stop trying to register
				Logger.Error("Could not inform root server of managers");
				Logger.Error(response.ErrorStackTrace);

				if (ReplicatedValueStore.IsConnectionFault(response))
					serviceTracker.ReportServiceDownClientReport(rootService, Net.ServiceType.Root);
			}
		}

		private void InformRootServerOfManagers(IServiceAddress rootServer) {
			// Make the managers list
			List<IServiceAddress> managers = new List<IServiceAddress>(64);
			lock (managerServers) {
				foreach (ManagerServerInfo m in managerServers) {
					managers.Add(m.Address);
				}
			}

			IServiceAddress[] managersSet = managers.ToArray();

			RequestMessage request = new RequestMessage("informOfManagers");
			request.Arguments.Add(managersSet);

			// Open a connection to the root server,
			IMessageProcessor processor = connector.Connect(rootServer, ServiceType.Root);
			MessageStream response = (MessageStream) processor.Process(request);
			if (response.HasError) {
				// If we failed, log a severe error but don't stop trying to register
				Logger.Error("Couldn't inform root server of managers");
				Logger.Error(response.ErrorStackTrace);

				if (ReplicatedValueStore.IsConnectionFault(response))
					serviceTracker.ReportServiceDownClientReport(rootServer, ServiceType.Root);
			}
		}

		private void RegisterBlockServer(IServiceAddress serviceAddress) {
			// Get the block server uid,
			RequestMessage request = new RequestMessage("serverGUID");

			// Connect to the block server,
			IMessageProcessor processor = connector.Connect(serviceAddress, ServiceType.Block);
			Message message_in = processor.Process(request);
			if (message_in.HasError)
				throw new ApplicationException(message_in.ErrorMessage);

			long server_guid = message_in.Arguments[0].ToInt64();

			// Add lookup for this server_guid <-> service address to the db,
			managerDb.SetValue("block.sguid." + server_guid, serviceAddress.ToString());
			managerDb.SetValue("block.addr." + serviceAddress, server_guid.ToString());

			// TODO: Block discovery on the introduced machine,




			// Set the status and guid
			BlockServerInfo block_server = new BlockServerInfo(server_guid, serviceAddress);
			// Add it to the map
			lock (blockServersMap) {
				blockServersMap[server_guid] = block_server;
				blockServers.Add(block_server);
				PersistBlockServers(blockServers);
			}
		}

		private void UnregisterBlockServer(IServiceAddress serviceAddress) {
			// Remove from the db,
			string block_addr_key = "block.addr." + serviceAddress;
			String server_sguid_str = managerDb.GetValue(block_addr_key);
			if (server_sguid_str != null) {
				managerDb.SetValue("block.sguid." + server_sguid_str, null);
				managerDb.SetValue(block_addr_key, null);
			}

			// Remove it from the map and persist
			lock (blockServersMap) {
				// Find the server to remove,
				List<BlockServerInfo> to_remove = new List<BlockServerInfo>();
				foreach (BlockServerInfo server in blockServers) {
					if (server.Address.Equals(serviceAddress)) {
						to_remove.Add(server);
					}
				}
				// Remove the entries that match,
				foreach (BlockServerInfo item in to_remove) {
					blockServersMap.Remove(item.Guid);
					blockServers.Remove(item);
				}
				PersistBlockServers(blockServers);
			}

			// Check that we aren't allocating against servers no longer in
			// the list. If so, fix the error.
			CheckAndFixAllocationServers();
		}

		private void UnregisterAllBlockServers() {
			// Create a list of servers to be deregistered,
			List<BlockServerInfo> toRemove;
			lock (blockServersMap) {
				toRemove = new List<BlockServerInfo>(blockServers.Count);
				toRemove.AddRange(blockServers);
			}

			// Remove all items in the to_remove from the db,
			foreach (BlockServerInfo item in toRemove) {
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
				foreach (BlockServerInfo item in toRemove) {
					blockServersMap.Remove(item.Guid);
					blockServers.Remove(item);
				}
				PersistBlockServers(blockServers);
			}

			// Check that we aren't allocating against servers no longer in
			// the list. If so, fix the error.
			CheckAndFixAllocationServers();
		}

		private void RegisterManagerServers(IServiceAddress[] serviceAddresses) {
			// Sanity check on number of manager servers (10 should be enough for
			// everyone !)
			if (serviceAddresses.Length > 100)
				throw new ApplicationException("Number of manager servers > 100");

			// Query all the manager servers on the network and generate a unique id
			// for this manager, if we need to create a new unique id,

			if (uniqueId == -1) {
				int sz = serviceAddresses.Length;
				List<int> blacklistId = new List<int>(sz);
				for (int i = 0; i < sz; ++i) {
					IServiceAddress man = serviceAddresses[i];
					if (!man.Equals(address)) {
						// Open a connection with the manager server,
						IMessageProcessor processor = connector.Connect(man, ServiceType.Manager);

						// Query the unique id of the manager server,
						MessageStream requestStream = new MessageStream(MessageType.Request);
						requestStream.AddMessage(new RequestMessage("getUniqueId"));
						MessageStream response = (MessageStream) processor.Process(requestStream);
						foreach (Message m in response) {
							if (m.HasError)
								throw new ApplicationException(m.ErrorMessage);

							long manUniqueId = m.Arguments[0].ToInt64();
							if (uniqueId == -1)
								throw new ApplicationException("getUniqueId = -1");

							// Add this to blacklist,
							blacklistId.Add((int) manUniqueId);
						}
					}
				}

				// Find a random id not found in the blacklist,
				int genId;
				while (true) {
					genId = random.Next(200);
					if (!blacklistId.Contains(genId)) {
						break;
					}
				}

				// Set the unique id,
				uniqueId = genId;
			}

			lock (managerServers) {
				managerServers.Clear();
				managerDb.ClearAllMachines();

				foreach (IServiceAddress serviceAddress in serviceAddresses) {
					if (!serviceAddress.Equals(address)) {
						ManagerServerInfo managerServer = new ManagerServerInfo(serviceAddress);
						managerServers.Add(managerServer);
						// Add to the manager database
						managerDb.AddMachine(managerServer.Address);
					}
				}

				PersistManagerServers(managerServers);
				PersistUniqueId(uniqueId);
			}

			// Perform initialization on the manager
			managerDb.Init();

			// Wait for initialization to complete,
			managerDb.WaitInitComplete();

			// Add a manager server entry,
			foreach (IServiceAddress manager_addr in serviceAddresses) {
				managerDb.SetValue("ms." + manager_addr, String.Empty);
			}

			// Tell all the root servers of the new manager set,
			List<IServiceAddress> root_servers_set = new List<IServiceAddress>(64);
			lock (rootServers) {
				foreach (RootServerInfo rs in rootServers) {
					root_servers_set.Add(rs.Address);
				}
			}
			foreach (IServiceAddress r in root_servers_set) {
				InformRootServerOfManagers(r);
			}
		}

		private void UnregisterManagerServer(IServiceAddress serviceAddress) {
			// Create a list of servers to be deregistered,
			List<ManagerServerInfo> toRemove;
			lock (managerServers) {
				toRemove = new List<ManagerServerInfo>(32);
				foreach (ManagerServerInfo item in managerServers) {
					if (item.Address.Equals(serviceAddress)) {
						toRemove.Add(item);
					}
				}
			}

			// Remove the entries and persist
			lock (managerServers) {
				// Remove the entries that match,
				foreach (ManagerServerInfo item in toRemove) {
					managerServers.Remove(item);
					// Add to the manager database
					managerDb.RemoveMachine(item.Address);
				}
				PersistManagerServers(managerServers);

				// Clear the unique id if we are deregistering this service,
				if (serviceAddress.Equals(address)) {
					uniqueId = -1;
					PersistUniqueId(uniqueId);
				}
			}

			// Perform initialization on the manager
			managerDb.Init();

			// Wait for initialization to complete,
			managerDb.WaitInitComplete();

			// Remove the manager server entry,
			managerDb.SetValue("ms." + serviceAddress, null);

			// Tell all the root servers of the new manager set,
			List<IServiceAddress> root_servers_set = new List<IServiceAddress>(64);
			lock (rootServers) {
				foreach (RootServerInfo rs in rootServers) {
					root_servers_set.Add(rs.Address);
				}
			}

			foreach (IServiceAddress r in root_servers_set) {
				InformRootServerOfManagers(r);
			}
		}

		private BlockServerInfo[] GetServersInfo(long[] serversGuid) {
			List<BlockServerInfo> reply;
			lock (blockServersMap) {
				int sz = serversGuid.Length;
				reply = new List<BlockServerInfo>(sz);
				for (int i = 0; i < sz; ++i) {
					BlockServerInfo blockServer;
					if (blockServersMap.TryGetValue(serversGuid[i], out blockServer)) {
						// Copy the server information into a new object.
						BlockServerInfo nbs = new BlockServerInfo(blockServer.Guid, blockServer.Address);
						reply.Add(nbs);
					}
				}
			}
			return reply.ToArray();
		}

		private void RegisterRootServer(IServiceAddress serviceAddress) {
			// The root server object,
			RootServerInfo root_server = new RootServerInfo(serviceAddress);

			// Add it to the map
			lock (rootServers) {
				rootServers.Add(root_server);
				PersistRootServers(rootServers);
			}

			// Add the root server entry,
			managerDb.SetValue("rs." + serviceAddress, String.Empty);

			// Tell root server about the managers.
			InformRootServerOfManagers(serviceAddress);
		}

		private void UnregisterRootServer(IServiceAddress serviceAddress) {
			// Remove it from the map and persist
			lock (rootServers) {
				// Find the server to remove,
				for (int i = rootServers.Count - 1; i >= 0; i--) {
					RootServerInfo server = rootServers[i];
					if (server.Address.Equals(serviceAddress)) {
						rootServers.RemoveAt(i);
					}
				}
				PersistRootServers(rootServers);
			}

			// Remove the root server entry,
			managerDb.SetValue("rs." + serviceAddress, null);

			// Tell root server about the managers.
			ClearRootServerOfManagers(serviceAddress);
		}

		private void UnregisterAllRootServers() {
			// Create a list of servers to be deregistered,
			List<RootServerInfo> toRemove;
			lock (rootServers) {
				toRemove = new List<RootServerInfo>(rootServers.Count);
				toRemove.AddRange(rootServers);
			}

			// Remove the entries from the map and persist
			lock (rootServers) {
				// Remove the entries that match,
				foreach (RootServerInfo item in toRemove) {
					rootServers.Remove(item);
				}
				PersistRootServers(rootServers);
			}

			// Clear the managers set from all the root servers,
			foreach (RootServerInfo item in toRemove) {
				ClearRootServerOfManagers(item.Address);
			}
		}

		private IServiceAddress GetRootForPath(string pathName) {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					PathRootTable path_root_map = new PathRootTable(transaction.GetFile(PathRootKey, FileAccess.Read));

					// Get the service address for the path name
					return path_root_map.Get(pathName);
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private  string[] GetPaths() {
			const string prefix = "path.info.";

			// Get all the keys with prefix 'path.info.'
			string[] pathKeys = managerDb.GetAllKeys(prefix);

			// Remove the prefix
			for (int i = 0; i < pathKeys.Length; ++i) {
				pathKeys[i] = pathKeys[i].Substring(prefix.Length);
			}

			return pathKeys;
		}

		private PathInfo GetPathInfo(string pathName) {
			string info = managerDb.GetValue("path.info." + pathName);

			if (String.IsNullOrEmpty(info))
				return null;

			// Create and return the path info object,
			return PathInfo.Parse(pathName, info);
		}

		private void AddPathToNetwork(string pathName, string pathType, IServiceAddress rootLeader, IServiceAddress[] rootServers) {
			if (pathType.Contains(","))
				throw new ApplicationException("Invalid path type");

			if (pathName.Contains(","))
				throw new ApplicationException("Invalid path name string");

			string key = "path.info." + pathName;
			// Check the map doesn't already exist
			if (managerDb.GetValue(key) != null)
				throw new ApplicationException("Path already assigned");

			// Set the first path info version for this path name
			PathInfo mpath_info = new PathInfo(pathName, pathType, 1, rootLeader, rootServers);

			// Add the path to the manager db cluster.
			managerDb.SetValue(key, mpath_info.ToString());
		}

		private void RemovePathFromNetwork(string path_name) {
			string key = "path.info." + path_name;

			// Remove the path from the manager db cluster,
			managerDb.SetValue(key, null);
		}

		private void RemovePathRootMapping(string pathName) {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					PathRootTable path_root_map = new PathRootTable(transaction.GetFile(PathRootKey, FileAccess.Write));
					path_root_map.Set(pathName, null);

					// Commit and check point the update,
					blockDatabase.Publish(transaction);
					blockDatabase.CheckPoint();
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private void AddPathRootMapping(string pathName, IServiceAddress serviceAddress) {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					PathRootTable path_root_map = new PathRootTable(transaction.GetFile(PathRootKey, FileAccess.Write));

					// Create the map
					path_root_map.Set(pathName, serviceAddress);

					// Commit and check point the update,
					blockDatabase.Publish(transaction);
					blockDatabase.CheckPoint();
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private void RemoveBlockServerMapping(BlockId blockId, long[] serverGuids) {
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

		private void AddBlockServerMapping(BlockId blockId, long[] serverGuids) {
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

		private void OnBlockIdCorrupted(IServiceAddress serverAddress, BlockId blockId, string failure_type) {
			// TODO:
		}

		private void OnBlockServerFailure(IServiceAddress serviceAddress) {
			// If the server currently recorded as up,
			if (serviceTracker.IsServiceUp(serviceAddress, ServiceType.Block)) {
				// Report the block service down to the service tracker,
				serviceTracker.ReportServiceDownClientReport(serviceAddress, ServiceType.Block);
			}

			// Change the allocation point if we are allocating against servers that
			// have failed,
			CheckAndFixAllocationServers();
		}

		private void NotifyBlockServerOfMaxBlockId(IServiceAddress blockServer, BlockId blockId) {
			if (serviceTracker.IsServiceUp(blockServer, ServiceType.Block)) {
				RequestMessage request = new RequestMessage("notifyCurrentBlockId");
				request.Arguments.Add(blockId);
				// Connect to the block server,
				IMessageProcessor processor = connector.Connect(blockServer, ServiceType.Block);
				Message message_in = processor.Process(request);
				// If the block server is down, report it to the tracker,
				if (message_in.HasError) {
					if (ReplicatedValueStore.IsConnectionFault(message_in)) {
						serviceTracker.ReportServiceDownClientReport(blockServer, ServiceType.Block);
					}
				}

			}
		}

		private void NotifyBlockServersOfCurrentBlockId(long[] block_servers_notify, BlockId block_id) {
			// Copy the block servers list for concurrency safety,
			List<BlockServerInfo> blockServersListCopy = new List<BlockServerInfo>(64);
			lock (blockServersMap) {
				blockServersListCopy.AddRange(blockServers);
			}

			// For each block server
			foreach (BlockServerInfo blockServer in blockServersListCopy) {
				// Is it in the block_servers_notify list?
				bool found = false;
				foreach (long bsn in block_servers_notify) {
					if (blockServer.Guid == bsn) {
						found = true;
						break;
					}
				}

				// If found and the service is up,
				if (found)
					NotifyBlockServerOfMaxBlockId(blockServer.Address, block_id);
			}
		}

		private long GetBlockMappingCount() {
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.Read));
					return blockServerTable.Count;
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private byte[] FindByteMapForBlocks(IServiceAddress serviceAddress, BlockId[] blocks) {
			IMessageProcessor processor = connector.Connect(serviceAddress, ServiceType.Block);

			RequestMessage request = new RequestMessage("createAvailabilityMapForBlocks");
			request.Arguments.Add(blocks);
			Message response = processor.Process(request);

			if (response.HasError)
				// If the block server generates an error, return an empty array,
				return new byte[0];

			// Return the availability map,
			return (byte[]) response.Arguments[0].Value;
		}

		protected void AddRegisteredBlockServer(long serverGuid, IServiceAddress address) {
			lock (blockServersMap) {
				BlockServerInfo block_server = new BlockServerInfo(serverGuid, address);

				// Add to the internal map/list
				blockServersMap[serverGuid] = block_server;
				blockServers.Add(block_server);
			}
		}

		protected void AddRegisteredRootServer(IServiceAddress address) {
			lock (rootServers) {
				RootServerInfo root_server = new RootServerInfo(address);

				// Add to the internal map/list
				rootServers.Add(root_server);
			}
		}

		protected void AddRegisteredManagerServer(IServiceAddress addr) {
			lock (managerServers) {
				ManagerServerInfo manager_server = new ManagerServerInfo(addr);

				// Add to the internal map/list
				managerServers.Add(manager_server);

				// Add to the manager database
				managerDb.AddMachine(addr);
			}
		}

		protected abstract void PersistBlockServers(IList<BlockServerInfo> servers);

		protected abstract void PersistRootServers(IList<RootServerInfo> servers);

		protected abstract void PersistManagerServers(IList<ManagerServerInfo> servers);

		protected abstract void PersistUniqueId(int unique_id);

		protected void SetBlockDatabase(IDatabase database) {
			blockDatabase = database;
			// Set the manager database,
			managerDb = new ReplicatedValueStore(address, connector, blockDatabase, blockDbWriteLock, serviceTracker);
		}

		protected override void OnStart() {
			// Create a list of manager addresses,
			lock (managerServers) {
				int sz = managerServers.Count;
				IServiceAddress[] managers = new IServiceAddress[sz];
				for (int i = 0; i < sz; ++i) {
					managers[i] = managerServers[i].Address;
				}
			}

			// Perform the initialization
			managerDb.Init();

			// Set the task where every 5 minutes we update a block service
			BlockUpdateTask task = new BlockUpdateTask(this);
			timer = new Timer(task.Execute, null, random.Next(8*1000) + (15*1000), random.Next(30*1000) + (5*60*1000));


			// When the sync finishes, 'connected' is set to true.
			base.OnStart();
		}

		protected override void OnStop() {
			// Cancel the block update task,
			timer.Dispose();
			// Stop the service tracker,
			serviceTracker.Stop();
			base.OnStop();
		}

		#region BlockUpdateTask

		private class BlockUpdateTask {
			private readonly ManagerService service;
			private bool init;
			private int blockIdIndex;
			private BlockId current_end_block;

			public BlockUpdateTask(ManagerService service) {
				this.service = service;
			}

			public void Execute(object state) {
				BlockServerInfo blockToCheck;

				// Cycle through the block servers list,
				lock (service.blockServersMap) {
					if (service.blockServersMap.Count == 0)
						return;

					if (!init)
						blockIdIndex = service.random.Next(service.blockServers.Count);

					blockToCheck = service.blockServers[blockIdIndex];
					++blockIdIndex;
					if (blockIdIndex >= service.blockServers.Count) {
						blockIdIndex = 0;
					}
					init = true;
				}

				// Notify the block server of the current block,
				BlockId currentBlockId;
				lock (service.allocationLock) {
					if (service.currentAddressSpaceEnd == null) {
						if (current_end_block == null) {
							current_end_block = service.GetCurrentBlockIdAlloc();
						}
						currentBlockId = current_end_block;
					} else {
						currentBlockId = service.currentAddressSpaceEnd.BlockId;
					}
				}

				// Notify the block server we are cycling through of the maximum block id.
				service.NotifyBlockServerOfMaxBlockId(blockToCheck.Address, currentBlockId);
			}
		}

		#endregion

		#region ManagerServerMessageProcessor

		class ManagerServerMessageProcessor : IMessageProcessor {
			public ManagerServerMessageProcessor(ManagerService service) {
				this.service = service;
			}

			private readonly ManagerService service;

			private DataAddress AllocateNode(int node_size) {

				if (node_size >= 65536)
					throw new ArgumentException("node_size too large");
				if (node_size < 0)
					throw new ArgumentException("node_size too small");

				BlockId block_id;
				int data_id;
				bool next_block = false;
				BlockId next_block_id = null;

				lock (service.allocationLock) {

					// Check address_space_end is initialized
					service.InitCurrentAddressSpaceEnd();

					// Set fresh allocation to false because we allocated off the
					// current address space,
					service.fresh_allocation = false;

					// Fetch the current block of the end of the address space,
					block_id = service.currentAddressSpaceEnd.BlockId;
					// Get the data identifier,
					data_id = service.currentAddressSpaceEnd.DataId;

					// The next position,
					int next_data_id = data_id;
					next_block_id = block_id;
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
				return new DataAddress(block_id, data_id);
			}

			private void GetRegisteredBlockServers(Message response) {
				// Populate the list of registered block servers
				long[] guids;
				IServiceAddress[] srvs;
				lock (service.blockServersMap) {
					int sz = service.blockServers.Count;
					guids = new long[sz];
					srvs = new IServiceAddress[sz];
					int i = 0;
					foreach (BlockServerInfo m in service.blockServers) {
						guids[i] = m.Guid;
						srvs[i] = m.Address;
						++i;
					}
				}

				// The reply message,
				response.Arguments.Add(guids);
				response.Arguments.Add(srvs);
			}

			private void GetRegisteredRootServers(Message response) {
				// Populate the list of registered root servers
				IServiceAddress[] srvs;
				lock (service.rootServers) {
					int sz = service.rootServers.Count;
					srvs = new IServiceAddress[sz];
					int i = 0;
					foreach (RootServerInfo m in service.rootServers) {
						srvs[i] = m.Address;
						++i;
					}
				}
				// The reply message,
				response.Arguments.Add(srvs);
			}

			private void GetRegisteredServerList(Message response) {
				// Populate the list of registered servers
				IServiceAddress[] srvs;
				int[] statusCodes;
				lock (service.blockServersMap) {
					int sz = service.blockServers.Count;
					srvs = new IServiceAddress[sz];
					statusCodes = new int[sz];
					int i = 0;
					foreach (BlockServerInfo m in service.blockServers) {
						srvs[i] = m.Address;
						statusCodes[i] = (int) service.serviceTracker.GetServiceCurrentStatus(m.Address, ServiceType.Block);
						++i;
					}
				}

				// Populate the reply message,
				response.Arguments.Add(srvs);
				response.Arguments.Add(statusCodes);
			}

			private BlockServerInfo[] GetServerList(BlockId blockId) {
				// Query the local database for the server list of the block.  If the
				// block doesn't exist in the database then it provisions it over the
				// network.

				long[] server_ids = service.GetOnlineServersWithBlock(blockId);

				// Resolve the server ids into server names and parse it as a reply
				int sz = server_ids.Length;

				// No online servers contain the block
				if (sz == 0)
					throw new ApplicationException("No online servers for block: " + blockId);

				BlockServerInfo[] reply = service.GetServersInfo(server_ids);

				service.Logger.Info(String.Format("getServersInfo replied {0} for {1}", new Object[] { reply.Length, blockId.ToString() }));

				return reply;
			}

			public Message Process(Message request) {
				Message response;
				if (MessageStream.TryProcess(this, request, out response))
					return response;

				response = ((RequestMessage) request).CreateResponse();

				// The messages in the stream,
				try {
					// Check the service isn't in a stop state,
					service.CheckErrorState();

					switch (request.Name) {
						case "getServerListForBlock": {
							BlockServerInfo[] servers = GetServerList((BlockId) request.Arguments[0].Value);
							response.Arguments.Add(servers.Length);
							for (int i = 0; i < servers.Length; ++i) {
								response.Arguments.Add(servers[i].Address);
								response.Arguments.Add(
									(int) service.serviceTracker.GetServiceCurrentStatus(servers[i].Address, ServiceType.Block));
							}
							break;
						}
						case "allocateNode": {
							int nodeSize = request.Arguments[0].ToInt32();
							DataAddress address = AllocateNode(nodeSize);
							response.Arguments.Add(address);
							break;
						}
						case "registerManagerServers": {
							service.RegisterManagerServers((IServiceAddress[])request.Arguments[0].Value);
							response.Arguments.Add(1L);
							break;
						}
						case "unregisterManagerServers": {
							service.UnregisterManagerServer((IServiceAddress)request.Arguments[0].Value);
							response.Arguments.Add(1L);
							break;
						}
						case "registerBlockServer": {
							IServiceAddress address = (IServiceAddress) request.Arguments[0].Value;
							service.RegisterBlockServer(address);
							response.Arguments.Add(1L);
							break;
						}
						case "unregisterBlockServer": {
							IServiceAddress address = (IServiceAddress) request.Arguments[0].Value;
							service.UnregisterBlockServer(address);
							response.Arguments.Add(1L);
							break;
						}
						case "unregisterAllBlockServers": {
							service.UnregisterAllBlockServers();
							response.Arguments.Add(1L);
							break;
						}

							// root servers
						case "registerRootServer": {
							IServiceAddress address = (IServiceAddress) request.Arguments[0].Value;
							service.RegisterRootServer(address);
							response.Arguments.Add(1L);
							break;
						}
						case "unregisterRootServer": {
							IServiceAddress address = (IServiceAddress) request.Arguments[0].Value;
							service.UnregisterRootServer(address);
							response.Arguments.Add(1L);
							break;
						}
						case "unregisterAllRootServers": {
							service.UnregisterAllRootServers();
							response.Arguments.Add(1L);
							break;
						}

						case "addPathToNetwork": {
							string pathName = request.Arguments[0].ToString();
							string pathType = request.Arguments[1].ToString();
							IServiceAddress rootLeader = (IServiceAddress) request.Arguments[2].Value;
							IServiceAddress[] rootServers = (IServiceAddress[]) request.Arguments[3].Value;
							service.AddPathToNetwork(pathName, pathType, rootLeader, rootServers);
							response.Arguments.Add(1L);
							break;
						}
						case "removePathFromNetwork": {
							service.RemovePathFromNetwork(request.Arguments[0].ToString());
							response.Arguments.Add(1L);
							break;
						}
						case "addBlockServerMapping": {
							service.AddBlockServerMapping((BlockId) request.Arguments[0].Value, (long[]) request.Arguments[1].Value);
							response.Arguments.Add(1L);
							break;
						}
						case "removeBlockServerMapping": {
							service.RemoveBlockServerMapping((BlockId) request.Arguments[0].Value, (long[]) request.Arguments[1].Value);
							response.Arguments.Add(1L);
							break;
						}
						case "getPathInfoForPath": {
							PathInfo pathInfo = service.GetPathInfo(request.Arguments[0].ToString());
							response.Arguments.Add(pathInfo);
							break;
						}
						case "getPaths": {
							string[] pathSet = service.GetPaths();
							response.Arguments.Add(pathSet);
							break;
						}
						case "getRegisteredServerList": {
							GetRegisteredServerList(response);
							break;
						}
						case "getRegisteredBlockServers": {
							GetRegisteredBlockServers(response);
							break;
						}
						case "getRegisteredRootServers": {
							GetRegisteredRootServers(response);
							break;
						}
						case "notifyBlockServerFailure": {
							IServiceAddress address = (IServiceAddress)request.Arguments[0].Value;
							service.OnBlockServerFailure(address);
							response.Arguments.Add(1L);
							break;
						}
						case "notifyBlockIdCorruption": {
							IServiceAddress serviceAddress = (IServiceAddress) request.Arguments[0].Value;
							BlockId blockId = (BlockId) request.Arguments[1].Value;
							string failureType = request.Arguments[2].ToString();
							service.OnBlockIdCorrupted(serviceAddress, blockId, failureType);
							break;
						}
						case "getUniqueId": {
							response.Arguments.Add(service.uniqueId);
							break;
						}
						case "poll": {
							service.managerDb.CheckConnected();
							response.Arguments.Add(1);
							break;
						}
						default:
							service.managerDb.Process(request, response);
							break;
					}
				} catch (OutOfMemoryException e) {
					service.Logger.Error("Out of Memory", e);
					service.SetErrorState(e);
					throw;
				} catch (Exception e) {
					service.Logger.Error("Error while processing message", e);
					response.Arguments.Add(new MessageError(e));
				}

				return response;
			}
		}

		#endregion

		#region RootServerInfo

		protected sealed class RootServerInfo {
			private readonly IServiceAddress address;

			internal RootServerInfo(IServiceAddress address) {
				this.address = address;
			}

			public IServiceAddress Address {
				get { return address; }
			}

			public override bool Equals(Object obj) {
				RootServerInfo serverInfo = (RootServerInfo)obj;
				return address.Equals(serverInfo.address);
			}

			public override int GetHashCode() {
				return address.GetHashCode();
			}
		}
		#endregion

		#region BlockServerInfo

		protected sealed class BlockServerInfo {
			private readonly long guid;
			private readonly IServiceAddress address;

			internal BlockServerInfo(long guid, IServiceAddress address) {
				this.guid = guid;
				this.address = address;
			}

			public long Guid {
				get { return guid; }
			}

			public IServiceAddress Address {
				get { return address; }
			}

			public override bool Equals(Object obj) {
				BlockServerInfo serverInfo = (BlockServerInfo)obj;
				return guid.Equals(serverInfo.guid);
			}

			public override int GetHashCode() {
				return guid.GetHashCode();
			}
		}


		#endregion

		#region ManagerServerInfo

		protected sealed class ManagerServerInfo {
			private readonly IServiceAddress address;

			public ManagerServerInfo(IServiceAddress address) {
				this.address = address;
			}

			public IServiceAddress Address {
				get { return address; }
			}
		}

		#endregion
	}
}