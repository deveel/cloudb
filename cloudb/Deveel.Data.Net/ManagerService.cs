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
		private IDatabase blockDatabase;
		private readonly object blockDbWriteLock = new object();
		private readonly Random random;

		private readonly object heartbeatLock = new object();
		private readonly Dictionary<IServiceAddress, DateTime> failureFloodControl;
		private readonly List<BlockServerInfo> monitoredServers = new List<BlockServerInfo>(32);
		private readonly Thread heartbeatThread;

		private static readonly Key BlockServerKey = new Key(12, 0, 10);
		private static readonly Key PathRootKey = new Key(12, 0, 20);

		protected ManagerService(IServiceConnector connector, IServiceAddress address) {
			this.connector = connector;
			this.address = address;

			blockServersMap = new Dictionary<long, BlockServerInfo>(256);
			blockServers = new List<BlockServerInfo>(256);
			rootServers = new List<RootServerInfo>(256);
			random = new Random();

			failureFloodControl = new Dictionary<IServiceAddress, DateTime>();

			heartbeatThread = new Thread(Heartbeat);
			heartbeatThread.IsBackground = true;
			heartbeatThread.Start();
		}

		~ManagerService() {
			Dispose(false);
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Manager; }
		}

		protected override IMessageProcessor CreateProcessor() {
			return new ManagerServerMessageProcessor(this);
		}

		private void UpdateAddressSpaceEnd() {
			lock (blockDbWriteLock) {
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.Read));

					// Set the 'current address space end' object with a value that is
					// past the end of the address space.

					// Fetch the last block added,
					DataAddress addressSpaceEnd = new DataAddress(0, 0);
					// If the map is empty,
					if (blockServerTable.Count != 0) {
						long lastBlockId = blockServerTable.LastBlockId;
						addressSpaceEnd = new DataAddress(lastBlockId + 1024, 0);
					}
					lock (allocationLock) {
						if (currentAddressSpaceEnd == null) {
							currentAddressSpaceEnd = addressSpaceEnd;
						} else {
							currentAddressSpaceEnd = currentAddressSpaceEnd.Max(addressSpaceEnd);
						}
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private long[] AllocateOnlineServerNodesForBlock(long blockId) {
			// Fetch the list of all online servers,
			List<BlockServerInfo> servSet = new List<BlockServerInfo>(blockServers.Count);
			lock (blockServersMap) {
				foreach (BlockServerInfo server in blockServers) {
					// Add the servers with status 'up'
					if (server.Status == ServiceStatus.Up)
						servSet.Add(server);
				}
			}

			// TODO: This is a simple random service picking method for a block.
			//   We should prioritize servers picked based on machine specs, etc.

			int sz = servSet.Count;
			// If serv_set is 3 or less, we return the servers available,
			if (sz <= 3) {
				long[] returnVal = new long[sz];
				for (int i = 0; i < sz; ++i)
					returnVal[i] = servSet[i].Guid;
				return returnVal;
			} else {
				// Randomly pick three servers from the list,
				long[] returnVal = new long[3];
				for (int i = 0; i < 3; ++i) {
					// java.util.Random is specced to be thread-safe,
					int randomIndex = random.Next(servSet.Count);
					BlockServerInfo blockServer = servSet[randomIndex];
					servSet.RemoveAt(randomIndex);
					returnVal[i] = blockServer.Guid;
				}

				// Return the array,
				return returnVal;
			}
		}

		private long[] GetOnlineServersWithBlock(long blockId) {
			long[] servers;
			// Note; we perform these operations inside a lock because we may need to
			//  provision servers to contain certain blocks which requires a database
			//  update.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.ReadWrite));

					// Get the servers list,
					servers = blockServerTable[blockId];
					// If the list is empty,
					if (servers.Length == 0) {
						// Provision servers to contain the block,
						servers = AllocateOnlineServerNodesForBlock(blockId);
						// Did we allocate any servers for this block?
						if (servers.Length > 0) {
							// Put the servers in the map,
							for (int i = 0; i < servers.Length; ++i)
								blockServerTable.Add(blockId, servers[i]);

							// Commit and check point the update,
							blockDatabase.Publish(transaction);
							blockDatabase.CheckPoint();
						}
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}

			return servers;
		}

		private void CheckAndFixAllocationServers() {
			// If the failure report is on a block service that is servicing allocation
			// requests, we push the allocation requests to the next block.
			long currentBlockId;
			lock (allocationLock) {
				currentBlockId = currentAddressSpaceEnd.BlockId;
			}

			long[] bservers = GetOnlineServersWithBlock(currentBlockId);

			int okServerCount = 0;

			// Change the status of the block service to STATUS_DOWN_CLIENT_REPORT
			lock (blockServersMap) {
				// For each service that stores the block,
				for (int i = 0; i < bservers.Length; ++i) {
					long serverGuid = bservers[i];
					// Is the status of this service UP?
					foreach (BlockServerInfo block_server in blockServers) {
						// If this matches the guid, and is up, we add to 'ok_server_count'
						if (block_server.Guid == serverGuid &&
							block_server.Status == ServiceStatus.Up) {
							++okServerCount;
						}
					}
				}
			}

			// If the count of ok servers for the allocation set size is not
			// the same then there are one or more servers that are inoperable
			// in the allocation set. So, we increment the block id ref of
			// 'current address space end' by 1 to force a reevaluation of the
			// servers to allocate the current block.
			if (okServerCount != bservers.Length) {
				lock (allocationLock) {
					long blockId = currentAddressSpaceEnd.BlockId;
					int dataId = currentAddressSpaceEnd.DataId;
					if (currentBlockId == blockId) {
						++blockId;
						dataId = 0;
						currentAddressSpaceEnd = new DataAddress(blockId, dataId);
					}
				}
			}
		}

		private void RegisterBlockServer(IServiceAddress serviceAddress) {
			// Open a connection with the block service,
			IMessageProcessor processor = connector.Connect(serviceAddress, ServiceType.Block);

			// Lock the block service with this manager
			RequestMessage request = new RequestMessage("bindWithManager");
			request.Arguments.Add(address);
			ResponseMessage response = processor.Process(request);
			if (response.HasError)
				throw new ApplicationException(response.ErrorMessage);

			// Get the block set report from the service,
			request = new RequestMessage("blockSetReport");
			response = processor.Process(request);

			if (response.HasError)
				throw new ApplicationException(response.ErrorMessage);
				
			long serverGuid = response.Arguments[0].ToInt64();
			long[] blockIdList = (long[])response.Arguments[1].Value;

			// Create a transaction
			lock (blockDbWriteLock) {
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.ReadWrite));

					int actualAdded = 0;

					// Read until the end
					int sz = blockIdList.Length;
					// Put each block item into the database,
					for (int i = 0; i < sz; ++i) {
						long block_id = blockIdList[i];
						bool added = blockServerTable.Add(block_id, serverGuid);
						if (added) {
							// TODO: Check if a service is adding polluted blocks into the
							//   network via checksum,
							++actualAdded;
						}
					}

					if (actualAdded > 0) {
						// Commit and check point the update,
						blockDatabase.Publish(transaction);
						blockDatabase.CheckPoint();
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}

			BlockServerInfo blockServer = new BlockServerInfo(serverGuid, serviceAddress, ServiceStatus.Up);
			// Add it to the map
			lock (blockServersMap) {
				blockServersMap[serverGuid] = blockServer;
				blockServers.Add(blockServer);
				PersistBlockServers(blockServers);
			}

			// Update the address space end variable,
			UpdateAddressSpaceEnd();
		}

		private void UnregisterBlockServer(IServiceAddress serviceAddress) {
			// Open a connection with the block service,
			IMessageProcessor processor = connector.Connect(serviceAddress, ServiceType.Block);

			// Unlock the block service from this manager
			RequestMessage request = new RequestMessage("unbindWithManager");
			request.Arguments.Add(address);
			ResponseMessage inputStream = processor.Process(request);
			if (inputStream.HasError)
				throw new ApplicationException(inputStream.ErrorMessage);

			// Remove it from the map and persist
			lock (blockServersMap) {
				// Find the service to remove,
				List<BlockServerInfo> to_remove = new List<BlockServerInfo>();
				foreach (BlockServerInfo server in blockServers) {
					if (server.Address.Equals(serviceAddress))
						to_remove.Add(server);
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
			List<BlockServerInfo> to_remove;
			lock (blockServersMap) {
				to_remove = new List<BlockServerInfo>(blockServers.Count);
				to_remove.AddRange(blockServers);
			}

			foreach (BlockServerInfo s in to_remove) {
				// Open a connection with the block service,
				IMessageProcessor processor = connector.Connect(s.Address, ServiceType.Block);

				// Unlock the block service from this manager
				RequestMessage request = new RequestMessage("unbindWithManager");
				request.Arguments.Add(address);
				ResponseMessage response = processor.Process(request);
				if (response.HasError)
					throw new ApplicationException(response.ErrorMessage);
			}

			// Remove the entries from the map and persist
			lock (blockServersMap) {
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

		private BlockServerInfo[] GetServersInfo(long[] servers_guid) {
			lock (blockServersMap) {
				int sz = servers_guid.Length;
				List<BlockServerInfo> reply = new List<BlockServerInfo>(sz);
				for (int i = 0; i < sz; ++i) {
					BlockServerInfo blockServer = blockServersMap[servers_guid[i]];
					if (blockServer != null)
						// Copy the service information into a new object.
						reply.Add(new BlockServerInfo(blockServer.Guid, blockServer.Address, blockServer.Status));
				}
				return reply.ToArray();
			}
		}

		private BlockServerInfo[] GetServerListForBlock(long blockId) {
			// Query the local database for the service list of the block.  If the
			// block doesn't exist in the database then it provisions it over the
			// network.

			long[] server_ids = GetOnlineServersWithBlock(blockId);

			// Resolve the service ids into service names and parse it as a reply
			int sz = server_ids.Length;

			// No online servers contain the block
			if (sz == 0)
				throw new ApplicationException("No online servers for block: " + blockId);

			return GetServersInfo(server_ids);
		}

		private void RegisterRootServer(IServiceAddress serviceAddress) {
			// Open a connection with the root service,
			IMessageProcessor processor = connector.Connect(serviceAddress, ServiceType.Root);

			// Lock the root service with this manager
			RequestMessage request = new RequestMessage("bindWithManager");
			request.Arguments.Add(address);
			ResponseMessage response = processor.Process(request);
			if (response.HasError)
				throw new ApplicationException(response.ErrorMessage);

			// Get the database path report from the service,
			request = new RequestMessage("pathReport");
			response = processor.Process(request);
			if (response.HasError)
				throw new ApplicationException(response.ErrorMessage);
				

			string[] pathsNames = (String[])response.Arguments[0].Value;

			// Create a transaction
			lock (blockDbWriteLock) {
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					PathRootTable pathRootTable = new PathRootTable(transaction.GetFile(PathRootKey, FileAccess.ReadWrite));

					// Read until the end
					int sz = pathsNames.Length;
					// Put each block item into the database,
					for (int i = 0; i < sz; ++i) {
						// Put the mapping of path_root to the root service that manages it.
						pathRootTable.Set(pathsNames[i], serviceAddress);
					}

					// Commit and check point the update,
					blockDatabase.Publish(transaction);
					blockDatabase.CheckPoint();

				} finally {
					blockDatabase.Dispose(transaction);
				}
			}

			// Add it to the map
			lock (rootServers) {
				rootServers.Add(new RootServerInfo(serviceAddress, ServiceStatus.Up));
				PersistRootServers(rootServers);
			}
		}

		private void UnregisterRootServer(IServiceAddress serviceAddress) {
			// Open a connection with the block service,
			IMessageProcessor processor = connector.Connect(serviceAddress, ServiceType.Root);

			// Unlock the block service from this manager
			RequestMessage request = new RequestMessage("unbindWithManager");
			request.Arguments.Add(address);
			ResponseMessage response = processor.Process(request);
			if (response.HasError)
				throw new ApplicationException(response.ErrorMessage);

			// Remove it from the map and persist
			lock (rootServers) {
				// Find the service to remove,
				for (int i = rootServers.Count - 1; i >= 0; i--) {
					if (rootServers[i].Address.Equals(serviceAddress))
						rootServers.RemoveAt(i);
				}

				PersistRootServers(rootServers);
			}
		}

		private void UnregisterAllRootServers() {
			// Create a list of servers to be deregistered,
			List<RootServerInfo> to_remove;
			lock (rootServers) {
				to_remove = new List<RootServerInfo>(rootServers.Count);
				to_remove.AddRange(rootServers);
			}

			foreach (RootServerInfo s in to_remove) {
				// Open a connection with the root service,
				IMessageProcessor processor = connector.Connect(s.Address, ServiceType.Root);

				// Unlock the root service from this manager
				RequestMessage request = new RequestMessage("unbindWithManager");
				request.Arguments.Add(address);
				ResponseMessage response = processor.Process(request);
				if (response.HasError)
					throw new ApplicationException(response.ErrorMessage);
			}

			// Remove the entries from the map and persist
			lock (rootServers) {
				// Remove the entries that match,
				foreach (RootServerInfo item in to_remove) {
					rootServers.Remove(item);
				}

				PersistRootServers(rootServers);
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
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			List<String> paths = new List<string>(32);
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					PathRootTable pathRootTable = new PathRootTable(transaction.GetFile(PathRootKey, FileAccess.Read));
					foreach(string path in pathRootTable.Keys) {
						paths.Add(path);
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
			// Return the list,
			return paths.ToArray();
		}

		private DataAddress AllocateNode(int nodeSize) {
			if (nodeSize >= 65536)
				throw new ArgumentException("node_size too large");
			if (nodeSize < 0)
				throw new ArgumentException("node_size too small");

			long blockId;
			int dataId;

			lock (allocationLock) {
				// Fetch the current block of the end of the address space,
				blockId = currentAddressSpaceEnd.BlockId;
				// Get the data identifier,
				dataId = currentAddressSpaceEnd.DataId;

				// The next position,
				int nextDataId = dataId;
				long nextBlockId = blockId;
				++nextDataId;
				if (nextDataId >= 16384) {
					nextDataId = 0;
					++nextBlockId;
				}

				// Create the new end address
				currentAddressSpaceEnd = new DataAddress(nextBlockId, nextDataId);
			}

			// Return the data address,
			return new DataAddress(blockId, dataId);
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

		private void RemoveBlockServerMapping(long blockId, long serverGuid) {
			lock (blockDbWriteLock) {
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.Write));
					blockServerTable.Remove(blockId, serverGuid);

					// Commit and check point the update,
					blockDatabase.Publish(transaction);
					blockDatabase.CheckPoint();
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private void AddBlockServerMapping(long blockId, long serverGuid) {
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.Write));

					// Make the block -> service map
					blockServerTable.Add(blockId, serverGuid);

					// Commit and check point the update,
					blockDatabase.Publish(transaction);
					blockDatabase.CheckPoint();
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private long[] GetServerGuidList(long blockId) {
			long[] servers;
			// Note; we perform these operations inside a lock because we may need to
			//  provision servers to contain certain blocks which requires a database
			//  update.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable blockServerTable = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.Read));

					// Get the servers list,
					servers = blockServerTable.Get(blockId);
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}

			return servers;
		}

		private void NotifyBlockServerFailure(IServiceAddress serviceAddress) {
			// This ensures that if we get flooded with failure notifications, the
			// load is not too great. Failure flooding should be capped by the
			// client also.
			lock (failureFloodControl) {
				DateTime currentTime = DateTime.Now;
				DateTime lastAddressFailTime;
				if (failureFloodControl.TryGetValue(serviceAddress, out lastAddressFailTime) &&
					lastAddressFailTime.AddMilliseconds(30 * 1000) > currentTime) {
					// We don't respond to failure notifications on the same address if a
					// failure notice arrived within a minute of the last one accepted.
					return;
				}
				failureFloodControl[serviceAddress] = currentTime;
			}

			// Change the status of the block service to STATUS_DOWN_CLIENT_REPORT
			lock (blockServersMap) {
				// Get the MSBlockServer object from the map,
				foreach (BlockServerInfo block_server in blockServers) {
					// If the block service is the one that failed,
					if (block_server.Address.Equals(serviceAddress)) {
						if (block_server.Status == ServiceStatus.Up) {
							block_server.Status = ServiceStatus.DownClientReport;
							// Add this block to the heartbeat check thread,
							MonitorServer(block_server);
						}
					}
				}
			}

			// Change the allocation point if we are allocating against servers that
			// have failed,
			CheckAndFixAllocationServers();
		}

		private long[] GetBlockMappingRange(long start, long end) {
			if (start < 0)
				throw new ArgumentException("start < 0");
			if (end < 0)
				throw new ArgumentException("end < 0");
			if (start > end)
				throw new ArgumentException("start > end");

			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// Get the map,
					BlockServerTable block_server_map = new BlockServerTable(transaction.GetFile(BlockServerKey, FileAccess.Read));
					long size = block_server_map.Count;
					start = Math.Min(start, size);
					end = Math.Min(end, size);

					// Return the range,
					return block_server_map.GetRange(start, end);
				} finally {
					blockDatabase.Dispose(transaction);
				}
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

		private void MonitorServer(BlockServerInfo blockServer) {
			lock (heartbeatLock) {
				if (!monitoredServers.Contains(blockServer))
					monitoredServers.Add(blockServer);
			}
		}

		private void PollServer(BlockServerInfo server) {
			bool pollOk = true;

			// Send the poll command to the service,
			IMessageProcessor p = connector.Connect(server.Address, ServiceType.Block);
			RequestMessage request = new RequestMessage("poll");
			request.Arguments.Add("manager heartbeat");
			ResponseMessage response = p.Process(request);
				// Any error with the poll means no status change,
				if (response.HasError) {
					pollOk = false;
			}

			// If the poll is ok, set the status of the service to UP and remove from
			// the monitor list,
			if (pollOk) {
				// The service status is set to 'Up' if either the current state
				// is 'DownClientReport' or 'DownHeartbeat'
				// Lock over servers map for safe alteration of the ref.
				lock (blockServersMap) {
					if (server.Status == ServiceStatus.DownClientReport ||
						server.Status == ServiceStatus.DownHeartbeat) {
						server.Status = ServiceStatus.Up;
					}
				}
				// Remove the service from the monitored_servers list.
				lock (heartbeatLock) {
					monitoredServers.Remove(server);
				}
			} else {
				// Make sure the service status is set to 'DownHeartbeat' if the poll
				// failed,
				// Lock over servers map for safe alteration of the ref.
				lock (blockServersMap) {
					if (server.Status == ServiceStatus.Up ||
						server.Status == ServiceStatus.DownClientReport) {
						server.Status = ServiceStatus.DownHeartbeat;
					}
				}
			}
		}

		private void Heartbeat() {
			try {
				while (true) {
					List<BlockServerInfo> servers;
					lock (heartbeatLock) {
						// Wait a minute,
						Monitor.Wait(heartbeatLock, 1 * 60 * 1000);
						// If there are no servers to monitor, continue the loop,
						if (monitoredServers.Count == 0)
							continue;

						// Otherwise, copy the monitored servers into the 'servers'
						// object,
						servers = new List<BlockServerInfo>(monitoredServers.Count);
						servers.AddRange(monitoredServers);
					}

					// Poll the servers
					foreach (BlockServerInfo s in servers) {
						PollServer(s);
					}
				}
			} catch (ThreadInterruptedException) {
				Logger.Warning("Heartbeat thread interrupted");
			}
		}
		
		protected void AddRegisteredBlockServer(long serverGuid, IServiceAddress address) {
			lock (blockServersMap) {
				BlockServerInfo blockServer = new BlockServerInfo(serverGuid, address, ServiceStatus.Up);
				// Add to the internal map/list
				blockServersMap[serverGuid] = blockServer;
				blockServers.Add(blockServer);
			}

			UpdateAddressSpaceEnd();
		}

		protected void AddRegisteredRootServer(IServiceAddress address) {
			lock (rootServers) {
				// Add to the internal map/list
				rootServers.Add(new RootServerInfo(address, ServiceStatus.Up));
			}
		}

		protected abstract void PersistBlockServers(IList<BlockServerInfo> servers_list);

		protected abstract void PersistRootServers(IList<RootServerInfo> servers_list);

		protected void SetBlockDatabase(IDatabase database) {
			blockDatabase = database;
		}

		#region ManagerServerMessageProcessor

		class ManagerServerMessageProcessor : IMessageProcessor {
			public ManagerServerMessageProcessor(ManagerService service) {
				this.service = service;
			}

			private readonly ManagerService service;

			public ResponseMessage Process(RequestMessage request) {
				ResponseMessage response;
				if (RequestMessageStream.TryProcess(this, request, out response))
					return response;

				response = request.CreateResponse();

				// The messages in the stream,
				try {
					// Check the service isn't in a stop state,
					service.CheckErrorState();

					switch (request.Name) {
						case "getServerListForBlock": {
							long blockId = request.Arguments[0].ToInt64();
							BlockServerInfo[] servers = service.GetServerListForBlock(blockId);
							IServiceAddress[] addresses = new IServiceAddress[servers.Length];
							int[] status = new int[servers.Length];
							response.Arguments.Add(servers.Length);
							for (int i = 0; i < servers.Length; i++) {
								addresses[i] = servers[i].Address;
								status[i] = (int) servers[i].Status;
							}
							response.Arguments.Add(addresses);
							response.Arguments.Add(status);
							break;
						}
						case "allocateNode": {
								int nodeSize = request.Arguments[0].ToInt32();
								DataAddress address = service.AllocateNode(nodeSize);
								response.Arguments.Add(address);
								break;
							}
						case "registerBlockServer": {
								IServiceAddress address = (IServiceAddress)request.Arguments[0].Value;
								service.RegisterBlockServer(address);
								response.Arguments.Add(1L);
								break;
							}
						case "unregisterBlockServer": {
								IServiceAddress address = (IServiceAddress)request.Arguments[0].Value;
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
								IServiceAddress address = (IServiceAddress)request.Arguments[0].Value;
								service.RegisterRootServer(address);
								response.Arguments.Add(1L);
								break;
							}
						case "unregisterRootServer": {
								IServiceAddress address = (IServiceAddress)request.Arguments[0].Value;
								service.UnregisterRootServer(address);
								response.Arguments.Add(1L);
								break;
							}
						case "unregisterAllRootServers": {
								service.UnregisterAllRootServers();
								response.Arguments.Add(1L);
								break;
							}
						case "getRootForPath": {
								string pathName = (string)request.Arguments[0].Value;
								IServiceAddress address = service.GetRootForPath(pathName);
								response.Arguments.Add(address);
								break;
							}
						case "addPathRootMapping": {
								string pathName = request.Arguments[0].ToString();
								IServiceAddress address = (IServiceAddress)request.Arguments[1].Value;
								service.AddPathRootMapping(pathName, address);
								response.Arguments.Add(1L);
								break;
							}
						case "removePathRootMapping": {
								string pathName = request.Arguments[0].ToString();
								service.RemovePathRootMapping(pathName);
								response.Arguments.Add(1L);
								break;
							}
						case "getPaths": {
								string[] pathSet = service.GetPaths();
								response.Arguments.Add(pathSet);
								break;
							}
						case "getServerGUIDList": {
								long blockId = request.Arguments[0].ToInt64();
								long[] serverGuids = service.GetServerGuidList(blockId);
								response.Arguments.Add(serverGuids);
								break;
							}
						case "addBlockServerMapping": {
								long blockId = request.Arguments[0].ToInt64();
								long serverGuid = request.Arguments[1].ToInt64();
								service.AddBlockServerMapping(blockId, serverGuid);
								response.Arguments.Add(1L);
								break;
							}
						case "removeBlockServerMapping": {
								long blockId = request.Arguments[0].ToInt64();
								long serverGuid = request.Arguments[1].ToInt64();
								service.RemoveBlockServerMapping(blockId, serverGuid);
								response.Arguments.Add(1L);
								break;
							}
						case "notifyBlockServerFailure": {
								IServiceAddress address = (IServiceAddress)request.Arguments[0].Value;
								service.NotifyBlockServerFailure(address);
								response.Arguments.Add(1L);
								break;
							}
						case "getBlockMappingCount": {
								long blockMappingCount = service.GetBlockMappingCount();
								response.Arguments.Add(blockMappingCount);
								break;
							}
						case "getBlockMappingRange": {
								long start = request.Arguments[0].ToInt64();
								long end = request.Arguments[1].ToInt64();
								long[] mappings = service.GetBlockMappingRange(start, end);
								response.Arguments.Add(mappings);
								break;
							}
						default:
							throw new ApplicationException("Unknown message name: " + request.Name);
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
			private readonly ServiceStatus status;

			internal RootServerInfo(IServiceAddress address, ServiceStatus status) {
				this.address = address;
				this.status = status;
			}

			public IServiceAddress Address {
				get { return address; }
			}

			public ServiceStatus Status {
				get { return status; }
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
			private ServiceStatus status;

			internal BlockServerInfo(long guid, IServiceAddress address, ServiceStatus status) {
				this.guid = guid;
				this.address = address;
				this.status = status;
			}

			public ServiceStatus Status {
				get { return status; }
				set { status = value; }
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
	}
}