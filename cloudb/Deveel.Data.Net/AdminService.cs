using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public class AdminService : Service {
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

		private void InitService(string  serviceTypeName) {
			InitService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName, true));
		}

		private void DisposeService(string  serviceTypeName) {
			DisposeService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName, true));
		}

		protected void InitService(ServiceType service_type) {
			// This service as a ServiceAddress object,
			// ServiceAddress this_service = new ServiceAddress(bind_interface, port);

			// Start the services,
			lock (serverManagerLock) {
				IService service = delegator.CreateService(address, service_type, connector);
				if (service == null)
					throw new Exception("Unable to create the service " + service_type);
				
				service.Init();
			}
		}

		protected void DisposeService(ServiceType service_type) {
			lock (serverManagerLock) {
				delegator.DisposeService(service_type);
			}
		}

		protected override void OnDispose(bool disposing) {
			if (disposing) {
				delegator.Dispose();
			}
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Admin; }
		}

		protected override IMessageProcessor CreateProcessor() {
			return new AdminServerMessageProcessor(this);
		}

		protected override void OnInit() {
			lock(serverManagerLock) {
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

			public MessageStream Process(MessageStream messageStream) {
				MessageStream outputStream = new MessageStream(32);

				// For each message in the message input,
				foreach (Message m in messageStream) {
					try {
						string command = m.Name;
						// Report on the services running,
						if (command.Equals("report")) {
							lock (service.serverManagerLock) {
								// TODO:
								long tm = 0;		// Total Memory
								long fm = 0;		// Free Memory
								long td = 0;		// Total Space
								long fd = 0;		// Free Space
								outputStream.StartMessage("R");
								if (service.Block == null) {
									outputStream.AddMessageArgument("block=no");
								} else {
									outputStream.AddMessageArgument(service.Block.BlockCount.ToString());
								}
								outputStream.AddMessageArgument("manager=" + (service.Manager == null ? "no" : "yes"));
								outputStream.AddMessageArgument("root=" + (service.Root == null ? "no" : "yes"));
								outputStream.AddMessageArgument(tm - fm);
								outputStream.AddMessageArgument(tm);
								outputStream.AddMessageArgument(td - fd);
								outputStream.AddMessageArgument(td);
								outputStream.CloseMessage();
							}
						} else if (command.Equals("reportStats")) {
							// Analytics stats; we convert the stats to a long[] array and
							// send it as a reply.
							long[] stats = GetStats();
							outputStream.AddMessage("R", stats);
						} else {
							// Starts a service,
							if (command.Equals("init")) {
								string service_type = (string)m[0];
								service.InitService(service_type);
							}
								// Stops a service,
							else if (command.Equals("dispose")) {
								string service_type = (String)m[0];
								service.DisposeService(service_type);
							} else {
								throw new Exception("Unknown command: " + command);
							}

							// Add reply message,
							outputStream.AddMessage("R", 1L);
						}

					} catch (OutOfMemoryException e) {
						service.Logger.Error(service, "Out Of Memory Error.");
						// This will end the connection);
						throw;
					} catch (Exception e) {
						service.Logger.Error("Error while processing.");
						outputStream.AddErrorMessage(new ServiceException(e));
					}
				}
				return outputStream;
			}
		}

		#endregion
	}
}