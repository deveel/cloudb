using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Deveel.Data.Net {
	public abstract class ManagerServer : IService {
		private bool disposed;
		private bool initialized;
		private IServiceConnector connector;
		private ServiceAddress address;
		private ErrorStateException errorState;
		
		private DataAddress currentAddressSpaceEnd;
		private readonly object allocationLock = new object();
		
		private readonly Dictionary<long, BlockServerInfo> blockServersMap;
		private readonly List<BlockServerInfo> blockServers;
		private readonly List<RootServerInfo> rootServers;
		private IDatabase blockDatabase;
		private readonly object blockDbWriteLock = new object();
		
		private readonly object heartbeatLock = new object();
		private List<BlockServerInfo> monitoredServers = new List<ManagerServer.BlockServerInfo>(32);
		private Thread heartbeatThread;
		
		private static Key BlockServerKey = new Key((short)12, 0, 10);
		
		protected ManagerServer(IServiceConnector connector, ServiceAddress address) {
			this.connector = connector;
			this.address = address;
			
			blockServersMap = new Dictionary<long, ManagerServer.BlockServerInfo>(256);
			blockServers = new List<BlockServerInfo>(256);
			rootServers = new List<RootServerInfo>(256);
			
			heartbeatThread = new Thread(Heartbeat);
			heartbeatThread.IsBackground = true;
			heartbeatThread.Start();
		}
		
		~ManagerServer() {
			Dispose(false);
		}
		
		public ServiceType ServiceType {
			get { return ServiceType.Manager; }
		}
		
		public IMessageProcessor Processor {
			get { return new ManagerServerMessageProcessor(this); }
		}
		
		private void Dispose(bool disposing) {
			if (!disposed) {
				OnDispose(disposing);
				disposed = true;
				initialized = false;
			}
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
		
		private void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}
		
		private void SetErrorState(Exception e) {
			errorState = new ErrorStateException(e);
		}
		
		private void RegisterBlockServer(ServiceAddress address) {
			// Open a connection with the block server,
			IMessageProcessor processor = connector.Connect(address, ServiceType.Block);

			// Lock the block server with this manager
			MessageStream inputStream, outputStream;
			outputStream = new MessageStream(16);
			outputStream.AddMessage("bindWithManager", this.address);
			inputStream = processor.Process(outputStream);
			foreach (Message m in inputStream) {
				if (m.IsError)
					throw new ApplicationException(m.ErrorMessage);
			}

			// Get the block set report from the server,
			outputStream = new MessageStream(16);
			outputStream.AddMessage("blockSetReport");
			inputStream = processor.Process(outputStream);
			Message rm = null;
			foreach (Message m in inputStream) {
				if (m.IsError) {
					throw new ApplicationException(m.ErrorMessage);
				} else {
					rm = m;
				}
			}
			
			long serverGuid = (long)rm[0];
			long[] blockIdList = (long[])rm[1];
			
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
							// TODO: Check if a server is adding polluted blocks into the
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

			BlockServerInfo blockServer = new BlockServerInfo(serverGuid, address, ServerStatus.Up);
			// Add it to the map
			lock (blockServersMap) {
				blockServersMap[serverGuid] = blockServer;
				blockServers.Add(blockServer);
				//TODO: how to update the list of servers?
			}

			// Update the address space end variable,
			UpdateAddressSpaceEnd();
		}
		
		private void UnregisterBlockServer(ServiceAddress address) {
			throw new NotImplementedException();
		}
		
		private void UnregisterAllBlockServers() {
			throw new NotImplementedException();
		}
		
		private BlockServerInfo[] GetServerListForBlock(long blockId) {
			throw new NotImplementedException();
		}
		
		private void RegisterRootServer(ServiceAddress address) {
			throw new NotImplementedException();
		}
		
		private void UnregisterRootServer(ServiceAddress address) {
			throw new NotImplementedException();
		}
		
		private void UnregisterAllRootServers() {
			throw new NotImplementedException();
		}
		
		private ServiceAddress GetRootForPath(string pathName) {
			throw new NotImplementedException();
		}
		
		private DataAddress AllocateNode(int nodeSize) {
			throw new NotImplementedException();
		}
		
		private void MonitorServer(BlockServerInfo blockServer) {
			lock (this) {
				if (!monitoredServers.Contains(blockServer))
					monitoredServers.Add(blockServer);
			}
		}
		
		private void PollServer(BlockServerInfo server) {
			bool pollOk = true;

			// Send the poll command to the server,
			IMessageProcessor p = connector.Connect(server.Address, ServiceType.Block);
			MessageStream msg_out = new MessageStream(16);
			msg_out.AddMessage("poll", "manager heartbeat");
			MessageStream msg_in = p.Process(msg_out);
			foreach (Message m in msg_in) {
				// Any error with the poll means no status change,
				if (m.IsError) {
					pollOk = false;
				}
			}

			// If the poll is ok, set the status of the server to UP and remove from
			// the monitor list,
			if (pollOk) {
				// The server status is set to 'Up' if either the current state
				// is 'DownClientReport' or 'DownHeartbeat'
				// Lock over servers map for safe alteration of the ref.
				lock (blockServersMap) {
					if (server.Status == ServerStatus.DownClientReport ||
						server.Status == ServerStatus.DownHeartbeat) {
						server.Status = ServerStatus.Up;
					}
				}
				// Remove the server from the monitored_servers list.
				lock (heartbeatLock) {
					monitoredServers.Remove(server);
				}
			} else {
				// Make sure the server status is set to 'DownHeartbeat' if the poll
				// failed,
				// Lock over servers map for safe alteration of the ref.
				lock (blockServersMap) {
					if (server.Status == ServerStatus.Up ||
						server.Status == ServerStatus.DownClientReport) {
						server.Status = ServerStatus.DownHeartbeat;
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
						Monitor.Wait(this, 1 * 60 * 1000);
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
				//TODO: WARN log ...
			}
		}
		
		protected void SetBlockDatabase(IDatabase database) {
			blockDatabase = database;
		}
		
		protected virtual void OnInit() {
		}
		
		protected virtual void OnDispose(bool disposing) {
		}
		
		public void Init() {
			if (initialized)
				throw new ApplicationException("The manager server was already initialized.");
			
			OnInit();
			
			initialized = true;
		}
		
		public void Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}
		
		#region ManagerServerMessageProcessor
		
		class ManagerServerMessageProcessor : IMessageProcessor {
			public ManagerServerMessageProcessor(ManagerServer server) {
				this.server = server;
			}
			
			private readonly ManagerServer server;
			
			public MessageStream Process(MessageStream messageStream) {
				MessageStream responseStream = new MessageStream(32);

				// The messages in the stream,
				foreach(Message m in messageStream) {
					try {
						// Check the server isn't in a stop state,
						server.CheckErrorState();
						
						string cmd = m.Name;
						switch(m.Name) {
							case "getServerListForBlock": {
								long  blockId = (long)m[0];
								BlockServerInfo[] servers = server.GetServerListForBlock(blockId);
								Message response = new Message("R");
								response.AddArgument(servers.Length);
								for(int i = 0; i < servers.Length; i++) {
									response.AddArgument(servers[i].Address);
									response.AddArgument((int)servers[i].Status);
								}
								responseStream.AddMessage(response);
								break;
							}
							case "allocateNode": {
								int nodeSize = (int)m[0];
								DataAddress address = server.AllocateNode(nodeSize);
								responseStream.AddMessage("R", address);
								break;
							}
							case "registerBlockServer": {
								ServiceAddress address = (ServiceAddress)m[0];
								server.RegisterBlockServer(address);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "unregisterBlockServer": {
								ServiceAddress address = (ServiceAddress)m[0];
								server.UnregisterBlockServer(address);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "unregisterAllBlockServers": {
								server.UnregisterAllBlockServers();
								responseStream.AddMessage("R", 1);
								break;	
							}
							
							// root servers
							case "registerRootServer": {
								ServiceAddress address = (ServiceAddress)m[0];
								server.RegisterRootServer(address);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "unregisterRootServer": {
								ServiceAddress address = (ServiceAddress)m[0];
								server.UnregisterRootServer(address);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "unregisterAllRootServers": {
								server.UnregisterAllRootServers();
								responseStream.AddMessage("R", 1);
								break;
							}
							case "getRootForPath": {
								string pathName = (string)m[0];
								ServiceAddress address = server.GetRootForPath(pathName);
								responseStream.AddMessage("R", address);
								break;
							}
							default:
								throw new ApplicationException("Unknown message name: " + m.Name);
						}
					} catch (OutOfMemoryException e) {
						//TODO: ERROR log ...
						server.SetErrorState(e);
						throw e;
					} catch (Exception e) {
						//TODO: ERROR log ...
						responseStream.AddErrorMessage(new ServiceException(e));
					}
				}

				return responseStream;
			}
		}
		
		#endregion
		
		#region RootServerInfo
		
		private class RootServerInfo {
			private ServiceAddress address;
			private ServerStatus status;
			
			public RootServerInfo(ServiceAddress address, ServerStatus status) {
				this.address = address;
				this.status = status;
			}
			
			public ServiceAddress Address {
				get { return address; }
			}
			
			public ServerStatus Status {
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
		
		private class BlockServerInfo {
			private long guid;
			private ServiceAddress address;
			private ServerStatus status;
			
			public BlockServerInfo(long guid, ServiceAddress address, ServerStatus status) {
				this.guid = guid;
				this.address = address;
				this.status = status;
			}
			
			public ServerStatus Status {
				get { return status; }
				set { status = value; }
			}
			
			public long Guid {
				get { return guid; }
			}
			
			public ServiceAddress Address {
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
		
		#region ServerStatus
		
		enum ServerStatus {
			Up = 1,
			DownShutdown = 2,
			DownHeartbeat = 3,
			DownClientReport = 4
		}
		
		#endregion
	}
}