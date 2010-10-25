using System;
using System.Threading;
using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public class AdminService : Service {
		private NetworkConfigSource config;
		private Timer configTimer;
		private readonly IServiceAddress address;
		private readonly Analytics analytics;
		private readonly IAdminServiceDelegator delegator;
		private IServiceConnector connector;
		private readonly object serverManagerLock = new object();

		public AdminService(IServiceAddress address, IServiceConnector connector, IAdminServiceDelegator delegator) {
			if (delegator == null)
				throw new ArgumentNullException("delegator");
			
			this.delegator = delegator;
			this.address = address;
			this.connector = connector;
			analytics = new Analytics();
		}

		~AdminService() {
			Dispose(false);
		}
		
		public IServiceAddress Address {
			get { return address; }
		}
		
		public IServiceConnector Connector {
			get { return connector; }
			set { connector = value; }
		}
		
		protected ManagerService Manager {
			get { return (ManagerService) delegator.GetService(ServiceType.Manager); }
		}

		protected RootService Root {
			get { return (RootService) delegator.GetService(ServiceType.Root); }
		}

		protected BlockService Block {
			get { return (BlockService) delegator.GetService(ServiceType.Block); }
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

		private void InitService(string  serviceTypeName) {
			InitService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName, true));
		}

		private void DisposeService(string  serviceTypeName) {
			DisposeService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName, true));
		}
		
		protected bool IsAddressAllowed(string address) {
			return config != null && config.IsIpAllowed(address);
		}

		protected void InitService(ServiceType serviceType) {
			// Start the services,
			lock (serverManagerLock) {
				IService service = delegator.CreateService(address, serviceType, connector);
				if (service == null)
					throw new Exception("Unable to create the service " + serviceType);
				
				service.Start();
			}
		}

		protected void DisposeService(ServiceType service_type) {
			lock (serverManagerLock) {
				delegator.DisposeService(service_type);
			}
		}

		protected override void OnStop() {
			if (configTimer != null) {
				configTimer.Dispose();
				configTimer = null;
			}

			if (delegator != null)
				delegator.Dispose();
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
				
				delegator.Init(this);
			}
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

			public ResponseMessage Process(RequestMessage request) {
				ResponseMessage response;
				if (RequestMessageStream.TryProcess(this, request, out response))
					return response;

				response = request.CreateResponse();

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
							service.InitService(service_type);
						}
							// Stops a service,
						else if (command.Equals("dispose")) {
							string service_type = request.Arguments[0].ToString();
							service.DisposeService(service_type);
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