using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public abstract class AdminServer : IService {
		private readonly Analytics analytics;

		private readonly object serverManagerLock = new object();
		private BlockServer blockServer;
		private ManagerServer managerServer;
		private RootServer rootServer;

		private bool disposed;

		protected AdminServer() {
			analytics = new Analytics();
		}

		~AdminServer() {
			Dispose(false);
		}

		protected ManagerServer Manager {
			get { return managerServer; }
		}

		protected RootServer Root {
			get { return rootServer; }
		}

		protected BlockServer Block {
			get { return blockServer; }
		}

		protected Analytics Analytics {
			get { return analytics; }
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					OnDispose(disposing);

					if (managerServer != null)
						managerServer.Dispose();
					if (rootServer != null)
						rootServer.Dispose();
					if (blockServer != null)
						blockServer.Dispose();

					managerServer = null;
					rootServer = null;
					blockServer = null;
				}
				disposed = true;
			}
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
					if (blockServer == null) {
						blockServer = (BlockServer)CreateService(Net.ServiceType.Block);
						blockServer.Init();
					}
				} else if (service_type == ServiceType.Manager) {
					if (managerServer == null) {
						managerServer = (ManagerServer)CreateService(Net.ServiceType.Manager);
						managerServer.Init();
					}
				} else if (service_type == ServiceType.Root) {
					if (rootServer == null) {
						rootServer = (RootServer) CreateService(Net.ServiceType.Root);
						rootServer.Init();
					}
				} else {
					throw new Exception("Unknown service: " + service_type);
				}
			}
		}

		protected void DisposeService(ServiceType service_type) {
			lock (serverManagerLock) {
				if (service_type == ServiceType.Block) {
					if (blockServer != null) {
						DisposeService(blockServer);
						blockServer = null;
					}
				} else if (service_type == ServiceType.Manager) {
					if (managerServer != null) {
						DisposeService(managerServer);
						managerServer = null;
					}
				} else if (service_type == ServiceType.Root) {
					if (rootServer != null) {
						DisposeService(rootServer);
						rootServer = null;
					}
				} else {
					throw new Exception("Unknown service: " + service_type);
				}
			}
		}

		protected abstract IService CreateService(ServiceType serviceType);

		protected abstract void DisposeService(IService service);

		protected virtual void OnInit() {
		}

		protected virtual void OnDispose(bool disposing) {
		}

		public void Dispose() {
			GC.SuppressFinalize(this);

			Dispose(true);
		}

		public ServiceType ServiceType {
			get { return ServiceType.Admin; }
		}

		public IMessageProcessor Processor {
			get { return new AdminServerMessageProcessor(this); }
		}

		public void Init() {
			lock(serverManagerLock) {
				OnInit();
			}
		}

		#region AdminServerMessageProcessor

		private sealed class AdminServerMessageProcessor : IMessageProcessor {
			private readonly AdminServer server;

			public AdminServerMessageProcessor(AdminServer server) {
				this.server = server;
			}

			private long[] GetStats() {
				AnalyticsRecord[] records = server.analytics.GetStats();
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
							lock (server.serverManagerLock) {
								// TODO:
								long tm = 0;		// Total Memory
								long fm = 0;		// Free Memory
								long td = 0;		// Total Space
								long fd = 0;		// Free Space
								outputStream.AddMessage("R");
								if (server.blockServer == null) {
									outputStream.AddMessageArgument("block=no");
								} else {
									outputStream.AddMessageArgument(server.blockServer.BlockCount.ToString());
								}
								outputStream.AddMessageArgument("manager=" + (server.managerServer == null ? "no" : "yes"));
								outputStream.AddMessageArgument("root=" + (server.rootServer == null ? "no" : "yes"));
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
								server.InitService(service_type);
							}
								// Stops a service,
							else if (command.Equals("dispose")) {
								string service_type = (String)m[0];
								server.DisposeService(service_type);
							} else {
								throw new Exception("Unknown command: " + command);
							}

							// Add reply message,
							outputStream.AddMessage("R", 1L);
						}

					} catch (OutOfMemoryException e) {
						//TODO: ERROR log ...
						// This will end the connection
						throw;
					} catch (Exception e) {
						//TODO: ERROR log ...
						outputStream.AddErrorMessage(new ServiceException(e));
					}
				}
				return outputStream;
			}
		}

		#endregion
	}
}