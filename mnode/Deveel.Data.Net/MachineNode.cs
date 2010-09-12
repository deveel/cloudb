using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

using Deveel.Configuration;

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
			//TODO: print an header and the product version ...

			string nodeConfig = null, netConfig = null;
			string hostArg = null, port_arg = null;

			StringWriter wout = new StringWriter();
			CommandLineOptions options = GetOptions();

			CommandLine command_line;

			bool failed = false;

			try {
				ICommandLineParser parser = CommandLineParser.CreateParse(ParseStyle.Gnu);
				command_line = parser.Parse(options, args);

				nodeConfig = command_line.GetOptionValue("nodeconfig", "node.conf");
				netConfig = command_line.GetOptionValue("netconfig", "network.conf");
				hostArg = command_line.GetOptionValue("host");
				port_arg = command_line.GetOptionValue("port");
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
			} else {
				// Get the node configuration file,
				ConfigSource node_config_resource = new ConfigSource();
				FileStream fin = new FileStream(nodeConfig, FileMode.Open, FileAccess.Read, FileShare.None);
				node_config_resource.Load(new BufferedStream(fin));
				fin.Close();

				// Parse the network configuration string,
				NetworkConfigSource net_config_resource;
				using(FileStream stream = new FileStream(netConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					net_config_resource = new NetworkConfigSource(stream);
				}

				string password = net_config_resource.GetString("network_password", null);
				if (password == null) {
					Console.Out.WriteLine("Error: couldn't determine the network password.");
					return 1;
				}

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

				string nodeDir = net_config_resource.GetString("node_directory", Environment.CurrentDirectory);

				Console.Out.WriteLine("Machine Node, " + (hostArg != null ? hostArg : "") + "port: " + port_arg);
				TcpAdminServer inst = new TcpFileAdminServer(net_config_resource, host, port, password, nodeDir);
				inst.Init();

				string line;
				while ((line = Console.In.ReadLine()) != null) {
					if (line.Equals("exit", StringComparison.InvariantCultureIgnoreCase)) {
						inst.Dispose();
						break;
					}
				}
			}

			return 0;
		}
	}
}