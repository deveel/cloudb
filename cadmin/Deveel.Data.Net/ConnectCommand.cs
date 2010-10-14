using System;

using Deveel.Configuration;
using Deveel.Console;
using Deveel.Console.Commands;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	internal class ConnectCommand : Command {
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			//TODO:
			return CommandResultCode.ExecutionFailed;
		}

		public override void RegisterOptions(Options options) {
			options.AddOption("protocol", true, "Specifies the connection protocol ('http' or 'tcp').");
			options.AddOption("host", true, "The address to the network manager host.");
			options.AddOption("port", true, "Inidicates the port of the manager service on the host.");
			options.AddOption("format", true, "Format used to serialize messages to/from the manager " +
			                                  "service ('xml', 'json' or 'binary')");
			options.AddOption("netpassword", true, "The challenge password used in all connection handshaking " +
			                                       "throughout the network. All machines must have the " +
			                                       "same net password.");
			options.AddOption("user", true, "");
			options.AddOption("password", true, "");
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			string protocol = commandLine.GetOptionValue("protocol", "tcp");
			string host = commandLine.GetOptionValue("host", null);
			string portArg = commandLine.GetOptionValue("port", null);
			string format = commandLine.GetOptionValue("format", "binary");

			NetworkContext context;

			if (String.IsNullOrEmpty(host) &&
				String.IsNullOrEmpty(portArg))
				return false;

			IServiceConnector connector;
			if (protocol.Equals("tcp", StringComparison.InvariantCultureIgnoreCase)) {
				string netPassword = commandLine.GetOptionValue("netpassword");
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
			((CloudAdmin)Application).SetNetworkContext(new NetworkContext(networkProfile));
			return true;
		}

		public override string Name {
			get { return "connect"; }
		}
	}
}