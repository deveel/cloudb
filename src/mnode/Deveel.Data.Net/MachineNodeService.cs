using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.ServiceProcess;

using Deveel.Configuration;
using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	partial class MachineNodeService : ServiceBase {
		private static TcpAdminService service;
		private readonly CommandLine commandLine;
		private readonly EventLog eventLog;

		public const string DisplayName = "CloudB Machine Node";
		public const string Name = "mnode";
		public const string Description = "The system service that makes this machine one of the nodes of a CloudB network.";

		public MachineNodeService(CommandLine commandLine) {
			this.commandLine = commandLine;

			eventLog = new EventLog("CloudB Machine Node", ".", "CloudB Machine Node");

			InitializeComponent();
		}

		private static IServiceFactory GetServiceFactory(string storage, ConfigSource nodeConfigSource) {
			if (storage == "file") {
				string nodeDir = nodeConfigSource.GetString("node_directory", Environment.CurrentDirectory);
				return new FileSystemServiceFactory(nodeDir);
			}
			if (storage == "memory")
				return new MemoryServiceFactory();

			if (String.IsNullOrEmpty(storage) &&
				nodeConfigSource != null) {
				storage = nodeConfigSource.GetString("storage", "file");
				return GetServiceFactory(storage, nodeConfigSource);
			}

			return null;
		}

		protected override void OnStart(string[] args) {
			eventLog.WriteEntry("Starting the service.", EventLogEntryType.Information);

			string nodeConfig, netConfig;
			string hostArg, portArg;

			try {
				nodeConfig = commandLine.GetOptionValue("nodeconfig", "node.conf");
				netConfig = commandLine.GetOptionValue("netconfig", "network.conf");
				hostArg = commandLine.GetOptionValue("host");
				portArg = commandLine.GetOptionValue("port");
			} catch (ParseException e) {
				eventLog.WriteEntry("Parse Error: " + e.Message, EventLogEntryType.Error);
				ExitCode = 1;
				throw new ApplicationException("Invalid arguments passed.");
			}

			try {
				// Get the node configuration file,
				ConfigSource nodeConfigSource = new ConfigSource();
				using (FileStream fin = new FileStream(nodeConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					nodeConfigSource.LoadProperties(new BufferedStream(fin));
				}

				// Parse the network configuration string,
				NetworkConfigSource netConfigSource;
				using (FileStream stream = new FileStream(netConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					netConfigSource = new NetworkConfigSource();
					//TODO: make it configurable ...
					netConfigSource.LoadProperties(stream);
				}

				string password = nodeConfigSource.GetString("network_password", null);
				if (password == null)
					throw new ApplicationException("Error: couldn't determine the network password.");

				// configure the loggers
				Logger.Init(nodeConfigSource);

				//TODO: support also IPv6

				// The base path,
				IPAddress host = null;
				if (hostArg != null) {
					IPAddress[] addresses = Dns.GetHostAddresses(hostArg);
					for (int i = 0; i < addresses.Length; i++) {
						IPAddress address = addresses[i];
						if (address.AddressFamily == AddressFamily.InterNetwork) {
							host = address;
							break;
						}
					}
				} else {
					host = IPAddress.Loopback;
				}

				if (host == null)
					throw new ApplicationException("Could not determine the host address.");

				int port;
				if (!Int32.TryParse(portArg, out  port))
					throw new ApplicationException("Error: couldn't parse port argument: " + portArg);

				string storage = commandLine.GetOptionValue("storage", null);
				IServiceFactory serviceFactory = GetServiceFactory(storage, nodeConfigSource);

				service = new TcpAdminService(serviceFactory, host, port, password);
				service.Config = netConfigSource;
				service.Start();

				eventLog.WriteEntry("TCP/IP service started successfully: " + host + ":" + port, EventLogEntryType.Information);
				eventLog.WriteEntry("Storage system: " + storage);
			} catch (Exception e) {
				if (service != null)
					service.Dispose();

				eventLog.WriteEntry("Error on start: " + e.Message, EventLogEntryType.Error);
				throw;
			}
		}

		protected override void OnStop() {
			eventLog.WriteEntry("Stopping the service", EventLogEntryType.Information);

			try {
				if (service != null) {
					service.Stop();
					service.Dispose();
					service = null;
				}

				eventLog.WriteEntry("Service successfully stopped");
			} catch (Exception e) {
				eventLog.WriteEntry("Error while stopping the service: " + e.Message, EventLogEntryType.Error);
				ExitCode = 1;
				throw;
			}
		}

		protected override void OnContinue() {
			eventLog.WriteEntry("Continue the execution of the service.");

			try {
				if (service != null)
					service.Start();

				eventLog.WriteEntry("Service continued its execution");
			} catch (Exception e) {
				eventLog.WriteEntry("Error on continue of the execution of the service: " + e.Message, EventLogEntryType.Error);
				throw;
			}
		}

		protected override void OnPause() {
			eventLog.WriteEntry("Entering in pause mode of the service.");

			try {
				if (service != null)
					service.Stop();
				eventLog.WriteEntry("Service paused");
			} catch (Exception e) {
				eventLog.WriteEntry("Error while pause the service: " + e.Message, EventLogEntryType.Error);
				throw;
			}
		}

		public void Start(string[] args) {
			OnStart(args);
		}
	}
}
