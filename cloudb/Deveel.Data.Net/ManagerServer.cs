using System;
using System.Collections.Generic;
using System.Threading;

namespace Deveel.Data.Net {
	public abstract class ManagerServer : IService {
		private bool disposed;
		private IServiceConnector connector;
		private ServiceAddress address;
		private ErrorStateException errorState;
		
		private readonly Dictionary<long, BlockServerInfo> blockServersMap;
		private readonly List<BlockServerInfo> blockServers;
		private readonly List<RootServerInfo> rootServers;
		
		private readonly object heartbeatLock = new object();
		private List<BlockServerInfo> monitoredServers = new List<ManagerServer.BlockServerInfo>(32);
		private Thread heartbeatThread;
		
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
				//TODO:
				disposed = true;
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
			throw new NotImplementedException();
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
		
		public void Init() {
			throw new NotImplementedException();
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