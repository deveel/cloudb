//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public class AdminService : Service {
		private NetworkConfigSource config;
		private Timer configTimer;
		private readonly IServiceAddress address;
		private readonly Analytics analytics;
		private readonly IServiceFactory serviceFactory;
		private IServiceConnector connector;
		private readonly object serverManagerLock = new object();

		private ManagerService manager;
		private RootService root;
		private BlockService block;

		private IMessageSerializer serializer;

		private Dictionary<string, ClientConnection> connections;

		public event ClientConnectionEventHandler ClientConnected;
		public event ClientConnectionEventHandler ClientDisconnected;
		public event ClientCommandEventHandler ClientRequest;
		public event ClientCommandEventHandler ClientResponse;

		public AdminService(IServiceAddress address, IServiceConnector connector, IServiceFactory serviceFactory) {
			if (serviceFactory == null) 
				throw new ArgumentNullException("serviceFactory");

			this.serviceFactory = serviceFactory;
			this.address = address;
			this.connector = connector;
			analytics = new Analytics();
		}

		protected virtual string Protocol {
			get { return null; }
		}
		
		public IServiceAddress Address {
			get { return address; }
		}
		
		public IServiceConnector Connector {
			get { return connector; }
			set { connector = value; }
		}
		
		protected ManagerService Manager {
			get { return manager; }
		}

		protected RootService Root {
			get { return root; }
		}

		protected BlockService Block {
			get { return block; }
		}

		protected Analytics Analytics {
			get { return analytics; }
		}
		
		public NetworkConfigSource Config {
			get { return config; }
			set { config = value; }
		}

		public IMessageSerializer MessageSerializer {
			get { return serializer ?? (serializer = new BinaryRpcMessageSerializer()); }
			set { serializer = value; }
		}

		private void ConfigUpdate(object state) {
			if (config != null)
				config.Reload();
		}

		private void StartService(string serviceTypeName) {
			StartService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName, true));
		}

		private void StopService(string serviceTypeName) {
			StopService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName, true));
		}
		
		protected bool IsAddressAllowed(string address) {
			return config != null && config.IsIpAllowed(address);
		}

		internal void OnClientResponse(ClientCommandEventArgs args) {
			if (ClientResponse != null)
				ClientResponse(this, args);
		}

		internal void OnClientRequest(ClientCommandEventArgs args) {
			if (ClientRequest != null)
				ClientRequest(this, args);
		}

		internal bool OnClientConnect(ClientConnectionEventArgs args) {
			if (ClientConnected != null)
				ClientConnected(this, args);

			return args.Authorized;
		}

		internal void OnClientDisconnect(ClientConnectionEventArgs args) {
			try {
				if (ClientDisconnected != null)
					ClientDisconnected(this, args);
			} finally {
				if (connections != null && connections.ContainsKey(args.RemoteEndPoint)) {
					connections.Remove(args.RemoteEndPoint);
					if (connections.Count == 0)
						connections = null;
				}
			}
		}

		protected bool OnClientConnect(string remoteEndPoint, bool authorized) {
			if (connections == null)
				connections = new Dictionary<string, ClientConnection>();

			ClientConnection connection = new ClientConnection(this, Protocol, remoteEndPoint);
			connections[remoteEndPoint] = connection;
			return connection.Connect(authorized);
		}

		protected virtual void OnClientDisconnect(string remoteEndPoint) {
			if (connections == null)
				return;

			ClientConnection connection;
			if (!connections.TryGetValue(remoteEndPoint, out connection))
				return;

			connection.Disconnect();
		}

		protected virtual void OnClientRequest(ServiceType serviceType, string remoteEndPoint, IEnumerable<Message> requestMessage) {
			if (connections == null)
				return;

			ClientConnection connection;
			if (!connections.TryGetValue(remoteEndPoint, out connection))
				return;

			connection.Request(serviceType, requestMessage);
		}

		protected void OnClientResponse(string remoteEndPoint, IEnumerable<Message> responseMessage) {
			if (connections == null)
				return;

			ClientConnection connection;
			if (!connections.TryGetValue(remoteEndPoint, out connection))
				return;

			connection.Response(responseMessage);
		}

		public void StartService(ServiceType serviceType) {
			// Start the services,
			lock (serverManagerLock) {
				IService service = serviceFactory.CreateService(address, serviceType, connector);
				if (service == null)
					throw new ApplicationException("Unable to create service of tyoe  " + serviceType);

				service.Start();

				if (serviceType == ServiceType.Manager)
					manager = (ManagerService)service;
				else if (serviceType == ServiceType.Root)
					root = (RootService)service;
				else if (serviceType == ServiceType.Block)
					block = (BlockService) service;
			}
		}

		public void StopService(ServiceType serviceType) {
			lock (serverManagerLock) {
				if (serviceType == ServiceType.Manager && manager != null) {
					manager.Stop();
					manager = null;
				} else if (serviceType == ServiceType.Root && root != null) {
					root.Stop();
					root = null;
				} else if (serviceType == ServiceType.Block && block != null) {
					block.Stop();
					block = null;
				}
			}
		}

		protected override void OnStop() {
			if (configTimer != null) {
				configTimer.Dispose();
				configTimer = null;
			}

			if (manager != null) {
				manager.Dispose();
				manager = null;
			}
			if (root != null) {
				root.Dispose();
				root = null;
			}
			if (block != null) {
				block.Dispose();
				block = null;
			}
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Admin; }
		}

		protected override IMessageProcessor CreateProcessor() {
			return new AdminServerMessageProcessor(this);
		}

		protected override void OnStart() {
			lock(serverManagerLock) {
				configTimer = new Timer(ConfigUpdate);
				
				// Schedule a refresh of the config file,
				// (We add a little entropy to ensure the network doesn't get hit by
				//  synchronized requests).
				Random r = new Random();
				long secondMix = r.Next(1000);
				configTimer.Change(50 * 1000, ((2 * 59) * 1000) + secondMix);
				
				serviceFactory.Init(this);
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (manager != null) {
					manager.Dispose();
					manager = null;
				}
				if (root != null) {
					root.Dispose();
					root = null;
				}
				if (block != null) {
					block.Dispose();
					block = null;
				}
			}

			base.Dispose(disposing);
		}

		#region AdminServerMessageProcessor

		private sealed class AdminServerMessageProcessor : IMessageProcessor {
			private readonly AdminService service;

			public AdminServerMessageProcessor(AdminService service) {
				this.service = service;
			}

			private long[] GetStats() {
				AnalyticsRecord[] records = service.analytics.GetStats();
				long[] stats = new long[records.Length*4];
				for (int i = 0; i < records.Length; i++) {
					AnalyticsRecord record = records[i];
					Array.Copy(record.ToArray(), 0, stats, i + 4, 4);
				}

				return stats;
			}

			public IEnumerable<Message> Process(IEnumerable<Message> request) {
				// The message output,
				MessageStream response = new MessageStream();

				// For each message in the message input,
				foreach (Message m in request) {
					try {
						string command = m.Name;
						// Report on the services running,
						if (command.Equals("report")) {
							lock (service.serverManagerLock) {
								long tm = System.Diagnostics.Process.GetCurrentProcess().PrivateMemorySize64;
								long fm = 0 /* TODO: GetFreeMemory()*/;
								long td = 0 /* TODO: GetTotalSpace(service.basePath) */;
								long fd = 0 /* TODO: GetUsableSpace(service.basePath)*/;

								MachineRoles roles = MachineRoles.None;
								Message message = new Message();
								if (service.block != null)
									roles |= MachineRoles.Block;
								if (service.manager != null)
									roles |= MachineRoles.Manager;
								if (service.root != null)
									roles |= MachineRoles.Root;

								message.Arguments.Add((byte) roles);
								message.Arguments.Add(tm - fm);
								message.Arguments.Add(tm);
								message.Arguments.Add(td - fd);
								message.Arguments.Add(td);
								response.AddMessage(message);
							}
						} else if (command.Equals("reportStats")) {
							// Analytics stats; we convert the stats to a long[] array and
							// send it as a reply.
							long[] stats = GetStats();
							response.AddMessage(new Message(new object[] {stats}));
						} else {
							// Starts a service,
							if (command.Equals("start")) {
								ServiceType serviceType = (ServiceType)(byte) m.Arguments[0].Value;
								service.StartService(serviceType);
							}
								// Stops a service,
							else if (command.Equals("stop")) {
								ServiceType serviceType = (ServiceType)(byte) m.Arguments[0].Value;
								service.StopService(serviceType);
							} else {
								throw new ApplicationException("Unknown command: " + command);
							}

							// Add reply message,
							response.AddMessage(new Message(1L));
						}

					} catch (OutOfMemoryException e) {
						service.Logger.Error("Out-Of-Memory", e);
						// This will end the connection
						throw;
					} catch (Exception e) {
						service.Logger.Error("Exception during process", e);
						response.AddMessage(new Message(new MessageError(e)));
					}
				}
				return response;

			}
		}

		#endregion
	}
}