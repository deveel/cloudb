using System;

using Deveel.Configuration;
using Deveel.Console;
using Deveel.Console.Commands;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	internal class ConnectCommand : Command {
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			string arg = args.Current;
			int index = arg.LastIndexOf('.');
			if (index != -1 && arg.Substring(index) == ".conf") {
				NetworkConfigSource configSource = new NetworkConfigSource(arg);
			} else {
				
			}
			
			return CommandResultCode.ExecutionFailed;
		}

		public override void RegisterOptions(Options options) {
			OptionGroup group = new OptionGroup();
			Option option = new Option("netconfig", true, "Either a path or URL of the location of the network " +
			                                              "configuration file (default: 'network.conf').");
			group.AddOption(option);
			option = new Option("address", true, "The address to a node of the network (typically a manager).");
			option.ArgumentCount = Option.UnlimitedValues;
			group.AddOption(option);
			options.AddOptionGroup(group);
			
			options.AddOption("protocol", true, "Specifies the connection protocol ('http' or 'tcp').");
			options.AddOption("format", true, "Format used to serialize messages to/from the manager " +
			                                  "service ('xml', 'json' or 'binary')");
			options.AddOption("password", true, "The challenge password used in all connection handshaking " +
			                                    "throughout the network.");
			options.AddOption("user", true, "The name of the user to authenticate in a HTTP connection.");
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			string protocol = commandLine.GetOptionValue("protocol", "tcp");
			string host = commandLine.GetOptionValue("host", null);
			string format = commandLine.GetOptionValue("format", "binary");
			
			string netConfig = commandLine.GetOptionValue("netconfig", null);
			string[] addresses = commandLine.GetOptionValues("address");

			if (String.IsNullOrEmpty(netConfig) &&
			    (addresses == null || addresses.Length == 0))
				return false;

			IServiceConnector connector;
			if (protocol.Equals("tcp", StringComparison.InvariantCultureIgnoreCase)) {
				string netPassword = commandLine.GetOptionValue("password");
				if (String.IsNullOrEmpty(netPassword))
					throw new ArgumentException("Netwrok password required for TCP/IP protocol.");

				connector = new TcpServiceConnector(netPassword);
			} else if (protocol.Equals("http", StringComparison.InvariantCultureIgnoreCase)) {
				string user = commandLine.GetOptionValue("user");
				string password = commandLine.GetOptionValue("password");
				if (String.IsNullOrEmpty(user))
					throw new ArgumentException("User name not specified. for HTTP connection.");
				if (String.IsNullOrEmpty(password))
					throw new ArgumentException("Password not specofoed for HTTP connection.");
				connector = new HttpServiceConnector(user, password);
			} else {
				throw new ArgumentException("Invalid protocol '" + protocol + "'.");
			}

			IMessageSerializer serializer;
			if (format.Equals("xml", StringComparison.InvariantCultureIgnoreCase)) {
				serializer = new XmlRpcMessageSerializer();
			} else if (format.Equals("binary", StringComparison.InvariantCultureIgnoreCase)) {
				serializer = new BinaryRpcMessageSerializer();
			} else if (format.Equals("json", StringComparison.InvariantCultureIgnoreCase)) {
				serializer = new JsonRpcMessageSerializer();
			} else {
				throw new ArgumentException("Invalid message format.");
			}

			connector.MessageSerializer = serializer;
			NetworkProfile networkProfile = new NetworkProfile(connector);
			if (String.IsNullOrEmpty(netConfig)) {
				NetworkConfigSource configSource = new NetworkConfigSource();
				for(int i = 0; i < addresses.Length; i++) {
					configSource.AddNetworkNode(addresses[i]);
				}
				networkProfile.Configuration = configSource;
			} else {
				NetworkConfigSource configSource = new NetworkConfigSource(netConfig);
				networkProfile.Configuration = configSource;
			}
			
			((CloudAdmin)Application).SetNetworkContext(new NetworkContext(networkProfile));
			return true;
		}

		public override string Name {
			get { return "connect"; }
		}
	}
}