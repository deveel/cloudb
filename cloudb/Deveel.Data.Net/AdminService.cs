using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public abstract class AdminService : Service {
		private readonly Analytics analytics;

		private readonly object serverManagerLock = new object();
		private BlockService blockService;
		private ManagerService managerService;
		private RootService rootService;

		protected AdminService() {
			analytics = new Analytics();
		}

		~AdminService() {
			Dispose(false);
		}

		protected ManagerService Manager {
			get { return managerService; }
		}

		protected RootService Root {
			get { return rootService; }
		}

		protected BlockService Block {
			get { return blockService; }
		}

		protected Analytics Analytics {
			get { return analytics; }
		}

		protected override object GetService(Type service) {
			if (typeof(BlockService).IsAssignableFrom(service))
				return blockService;
			if (typeof(ManagerService).IsAssignableFrom(service))
				return managerService;
			if (typeof(RootService).IsAssignableFrom(service))
				return rootService;

			return null;
		}

		private void InitService(string  serviceTypeName) {
			InitService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName));
		}

		private void DisposeService(string  serviceTypeName) {
			DisposeService((ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeName));
		}

		protected void InitService(ServiceType service_type) {
			// This service as a ServiceAddress object,
			// ServiceAddress this_service = new ServiceAddress(bind_interface, port);

			// Start the services,
			lock (serverManagerLock) {
				if (service_type == ServiceType.Block) {
					if (blockService == null) {
						blockService = (BlockService)CreateService(Net.ServiceType.Block);
						blockService.Init();
					}
				} else if (service_type == ServiceType.Manager) {
					if (managerService == null) {
						managerService = (ManagerService)CreateService(Net.ServiceType.Manager);
						managerService.Init();
					}
				} else if (service_type == ServiceType.Root) {
					if (rootService == null) {
						rootService = (RootService) CreateService(Net.ServiceType.Root);
						rootService.Init();
					}
				} else {
					throw new Exception("Unknown service: " + service_type);
				}
			}
		}

		protected void DisposeService(ServiceType service_type) {
			lock (serverManagerLock) {
				if (service_type == ServiceType.Block) {
					if (blockService != null) {
						DisposeService(blockService);
						blockService = null;
					}
				} else if (service_type == ServiceType.Manager) {
					if (managerService != null) {
						DisposeService(managerService);
						managerService = null;
					}
				} else if (service_type == ServiceType.Root) {
					if (rootService != null) {
						DisposeService(rootService);
						rootService = null;
					}
				} else {
					throw new Exception("Unknown service: " + service_type);
				}
			}
		}

		protected abstract IService CreateService(ServiceType serviceType);

		protected abstract void DisposeService(IService service);

		protected override void OnDispose(bool disposing) {
			if (disposing) {
				if (managerService != null)
					managerService.Dispose();
				if (rootService != null)
					rootService.Dispose();
				if (blockService != null)
					blockService.Dispose();

				managerService = null;
				rootService = null;
				blockService = null;
			}
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Admin; }
		}

		protected override IMessageProcessor CreateProcessor() {
			return new AdminServerMessageProcessor(this);
		}

		public new void Init() {
			lock(serverManagerLock) {
				OnInit();
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
								outputStream.AddMessage("R");
								if (service.blockService == null) {
									outputStream.AddMessageArgument("block=no");
								} else {
									outputStream.AddMessageArgument(service.blockService.BlockCount.ToString());
								}
								outputStream.AddMessageArgument("manager=" + (service.managerService == null ? "no" : "yes"));
								outputStream.AddMessageArgument("root=" + (service.rootService == null ? "no" : "yes"));
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
						service.Logger.Log(LogLevel.Error, service, "Out Of Memory Error.");
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