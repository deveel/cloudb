using System;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

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

		protected virtual void OnClientRequest(ServiceType serviceType, string remoteEndPoint, Message requestMessage) {
			if (connections == null)
				return;

			ClientConnection connection;
			if (!connections.TryGetValue(remoteEndPoint, out connection))
				return;

			connection.Request(serviceType, requestMessage);
		}

		protected void OnClientResponse(string remoteEndPoint, Message responseMessage) {
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
				long second_mix = r.Next(1000);
				configTimer.Change(50 * 1000, ((2 * 59) * 1000) + second_mix);
				
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
				long[] stats = new long[records.Length * 4];
				for (int i = 0; i < records.Length; i++) {
					AnalyticsRecord record = records[i];
					Array.Copy(record.ToArray(), 0, stats, i + 4, 4);
				}

				return stats;
			}

			public Message Process(Message request) {
				Message response;
				if (MessageStream.TryProcess(this, request, out response))
					return response;

				response = ((RequestMessage)request).CreateResponse();

				// For each message in the message input,
				try {
					string command = request.Name;
					// Report on the services running,
					if (command.Equals("report")) {
						lock (service.serverManagerLock) {
							// TODO:
							long tm = 0;		// Total Memory
							long fm = 0;		// Free Memory
							long td = 0;		// Total Space
							long fd = 0;		// Free Space
							if (service.Block == null) {
								response.Arguments.Add("block=no");
							} else {
								response.Arguments.Add(service.Block.BlockCount.ToString());
							}
							response.Arguments.Add("manager=" + (service.Manager == null ? "no" : "yes"));
							response.Arguments.Add("root=" + (service.Root == null ? "no" : "yes"));
							response.Arguments.Add(tm - fm);
							response.Arguments.Add(tm);
							response.Arguments.Add(td - fd);
							response.Arguments.Add(td);
						}
					} else if (command.Equals("reportStats")) {
						// Analytics stats; we convert the stats to a long[] array and
						// send it as a reply.
						long[] stats = GetStats();
						response.Arguments.Add(stats);
					} else {
						// Starts a service,
						if (command.Equals("init")) {
							string service_type = request.Arguments[0].ToString();
							service.StartService(service_type);
						}
							// Stops a service,
						else if (command.Equals("dispose")) {
							string service_type = request.Arguments[0].ToString();
							service.StopService(service_type);
						} else {
							throw new Exception("Unknown command: " + command);
						}

						// Add reply message,
						response.Arguments.Add(1L);
					}

				} catch (OutOfMemoryException e) {
					service.Logger.Error(service, "Out Of Memory Error.");
					// This will end the connection);
					throw;
				} catch (Exception e) {
					service.Logger.Error("Error while processing.");
					response.Arguments.Add(new MessageError(e));
				}

				return response;
			}
		}

		#endregion
	}
}