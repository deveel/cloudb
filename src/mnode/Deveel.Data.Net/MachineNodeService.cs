using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.ServiceProcess;

using Deveel.Configuration;
using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	internal class MachineNodeService : ServiceBase {
		private static TcpAdminService service;

		public const string DisplayName = "CloudB Machine Node";
		public const string Name = "mnode";
		public const string Description = "The system service that makes this machine one of the nodes of a CloudB network.";

		public MachineNodeService() {
			ServiceName = Name;
			AutoLog = true;

			CanPauseAndContinue = true;
			CanStop = true;
		}

		private static Options GetOptions() {
			Options options = new Options();
			options.AddOption("nodeconfig", true, "The node configuration file (default: node.conf).");
			options.AddOption("netconfig", true, "The network configuration file (default: network.conf).");
			options.AddOption("host", true, "The interface address to bind the socket on the local machine " +
							  "(optional - if not given binds to all interfaces)");
			options.AddOption("port", true, "The port to bind the socket.");
			options.AddOption("storage", true, "The type of storage used to persist node information and data");
			options.AddOption("protocol", true, "The connection protocol used by this node to listen connections");
			return options;
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
			CommandLine commandLine;
			Options options = GetOptions();

			string nodeConfig, netConfig;
			string hostArg, portArg;

			try {
				ICommandLineParser parser = new GnuParser(options);
				commandLine = parser.Parse(args);

				nodeConfig = commandLine.GetOptionValue("nodeconfig", "node.conf");
				netConfig = commandLine.GetOptionValue("netconfig", "network.conf");
				hostArg = commandLine.GetOptionValue("host");
				portArg = commandLine.GetOptionValue("port");
			} catch (ParseException) {
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
					throw new ApplicationException("Error: couldn't determine the host address.");

				int port;
				if (!Int32.TryParse(portArg, out  port))
					throw new ApplicationException("Error: couldn't parse port argument: " + portArg);

				string storage = commandLine.GetOptionValue("storage", null);
				IServiceFactory serviceFactory = GetServiceFactory(storage, nodeConfigSource);

				Console.Out.WriteLine("Machine Node, " + host + " : " + port);
				service = new TcpAdminService(serviceFactory, host, port, password);
				service.Config = netConfigSource;
				service.Start();
			} catch (Exception) {
				if (service != null)
					service.Dispose();
				throw;
			}
		}

		protected override void OnStop() {
			if (service != null) {
				service.Stop();
				service.Dispose();
				service = null;
			}
		}

		protected override void OnShutdown() {
			OnStop();
		}

		protected override void OnContinue() {
			if (service != null)
				service.Start();
		}

		protected override void OnPause() {
			if (service != null)
				service.Stop();
		}
	}
}