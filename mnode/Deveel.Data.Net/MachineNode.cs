using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

using Deveel.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Shell;

namespace Deveel.Data.Net {
	public static class MachineNode {
		private static CommandLineOptions GetOptions() {
			CommandLineOptions options = new CommandLineOptions();
			options.AddOption("nodeconfig", true, "The node configuration file (default: node.conf).");
			options.AddOption("netconfig", true, "The network configuration file (default: network.conf).");
			options.AddOption("host", true, "The interface address to bind the socket on the local machine " +
							  "(optional - if not given binds to all interfaces)");
			options.AddOption("port", true, "The port to bind the socket.");
			return options;
		}

		[STAThread]
		private static int Main(string[] args) {
			ProductInfo libInfo = ProductInfo.GetProductInfo(typeof(TcpFileAdminService));
			ProductInfo nodeInfo = ProductInfo.GetProductInfo(typeof(MachineNode));

			Console.Out.WriteLine("{0} {1} ( {2} )", nodeInfo.Title, nodeInfo.Version, nodeInfo.Copyright);
			Console.Out.WriteLine(nodeInfo.Description);
			Console.Out.WriteLine();
			Console.Out.WriteLine("{0} {1} ( {2} )", libInfo.Title, libInfo.Version, libInfo.Copyright);

			string nodeConfig = null, netConfig = null;
			string hostArg = null, port_arg = null;

			StringWriter wout = new StringWriter();
			CommandLineOptions options = GetOptions();

			CommandLine commandLine;

			bool failed = false;

			try {
				ICommandLineParser parser = CommandLineParser.CreateParse(ParseStyle.Gnu);
				commandLine = parser.Parse(options, args);

				nodeConfig = commandLine.GetOptionValue("nodeconfig", "node.conf");
				netConfig = commandLine.GetOptionValue("netconfig", "network.conf");
				hostArg = commandLine.GetOptionValue("host");
				port_arg = commandLine.GetOptionValue("port");
			} catch(CommandLineParseException) {
				wout.WriteLine("Error parsing arguments.");
				failed = true;
			}

			// Check arguments that can be null,
			if (netConfig == null) {
				wout.WriteLine("Error, no network configuration given.");
				failed = true;
			} else if (nodeConfig == null) {
				wout.WriteLine("Error, no node configuration file given.");
				failed = true;
			}
			if (port_arg == null) {
				wout.WriteLine("Error, no port address given.");
				failed = true;
			}

			wout.Flush();

			// If failed,
			if (failed) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.WriteHelp(Console.WindowWidth, "mnode", "", options, "");
				Console.Out.WriteLine();
				Console.Out.WriteLine(wout.ToString());
				return 1;
			}

			try {
				// Get the node configuration file,
				ConfigSource nodeConfigSource = new ConfigSource();
				using (FileStream fin = new FileStream(nodeConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					nodeConfigSource.Load(new BufferedStream(fin));
				}

				// Parse the network configuration string,
				NetworkConfigSource netConfigSource;
				using(FileStream stream = new FileStream(netConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					netConfigSource = new NetworkConfigSource(stream);
				}

				string password = netConfigSource.GetString("network_password", null);
				if (password == null) {
					Console.Out.WriteLine("Error: couldn't determine the network password.");
					return 1;
				}

				// configure the loggers
				LogManager.Init(nodeConfigSource);

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

				if (host == null) {
					Console.Out.WriteLine("Error: couldn't determine the host address.");
					return 1;
				}

				int port;
				if (!Int32.TryParse(port_arg, out  port)) {
					Console.Out.WriteLine("Error: couldn't parse port argument: " + port_arg);
					return 1;
				}

				if (hostArg != null)
					hostArg = hostArg + " ";

				string nodeDir = netConfigSource.GetString("node_directory", Environment.CurrentDirectory);

				Console.Out.WriteLine("Machine Node, " + (hostArg != null ? hostArg : "") + "port: " + port_arg);
				TcpAdminService inst = new TcpFileAdminService(netConfigSource, host, port, password, nodeDir);
				inst.Init();
			} catch(Exception e) {
				Console.Out.WriteLine(e.Message);
				Console.Out.WriteLine(e.StackTrace);
			}

			return 0;
		}
	}
}