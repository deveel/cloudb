using System;
using System.Collections.Generic;

using Deveel.Configuration;
using Deveel.Console;
using Deveel.Console.Commands;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	internal class ConnectCommand : Command {
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			if (args.Current != "to")
				return CommandResultCode.SyntaxError;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string address = args.Current;

			NetworkConfigSource configSource = new NetworkConfigSource();

			try {
				configSource.AddNetworkNode(address);
			} catch(Exception e) {
				Error.WriteLine("The address '" + address + "' is invalid: " + e.Message);
				return CommandResultCode.ExecutionFailed;
			}

			string protocol = "tcp";
			string credentials = String.Empty;
			string format = "binary";

			if (args.MoveNext()) {
				if (args.Current == "identified") {
					if (!args.MoveNext())
						return CommandResultCode.SyntaxError;
					if (args.Current != "by")
						return CommandResultCode.SyntaxError;
					if (!args.MoveNext())
						return CommandResultCode.SyntaxError;

					credentials = args.Current;

					if (args.MoveNext()) {
						if (args.Current != "on")
							return CommandResultCode.SyntaxError;

						protocol = args.Current;

						if (args.MoveNext()) {
							if (args.Current != "with")
								return CommandResultCode.SyntaxError;

							format = args.Current;
						}
					}
				} else if (args.Current == "on") {
					if (!args.MoveNext())
						return CommandResultCode.SyntaxError;

					protocol = args.Current;

					if (args.MoveNext()) {
						if (args.Current != "with")
							return CommandResultCode.SyntaxError;

						format = args.Current;
					}
				} else if (args.Current == "with") {
					if (!args.MoveNext())
						return CommandResultCode.SyntaxError;

					format = args.Current;
				} else {
					return CommandResultCode.SyntaxError;
				}
			}

			//TODO: is password is null, ask ...
			if (String.IsNullOrEmpty(credentials))
				return CommandResultCode.SyntaxError;

			IServiceConnector connector;
			if (protocol == "tcp") {
				connector = new TcpServiceConnector(credentials);
			} else if (protocol == "http") {
				string userName = credentials;
				string password = null;
				int index = credentials.IndexOf(':');
				if (index != -1) {
					password = credentials.Substring(index + 1);
					userName = credentials.Substring(0, index);
				}
				connector = new HttpServiceConnector(userName, password);
			} else {
				return CommandResultCode.SyntaxError;
			}

			IMessageSerializer serializer;

			if (format == "binary") {
				serializer = new BinaryRpcMessageSerializer();
			} else if (format == "xml") {
				serializer = new XmlRpcMessageSerializer();
			} else if (format == "json") {
				serializer = new JsonRpcMessageSerializer();
			} else {
				return CommandResultCode.SyntaxError;
			}

			connector.MessageSerializer = serializer;

			NetworkProfile networkProfile = new NetworkProfile(connector);
			networkProfile.Configuration = configSource;
			
			((CloudAdmin)Application).SetNetworkContext(new NetworkContext(networkProfile));
			return CommandResultCode.Success;
		}

		public override IEnumerator<string> Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			List<string> list = new List<string>();
			if (lastWord == "connect") {
				list.Add("to");
			} else if (lastWord == "identified") {
				list.Add("by");
			}

			return list.GetEnumerator();
		}

		public override void RegisterOptions(Options options) {
			options.AddOption("address", true, "The address to a node of the network (typically a manager).");			
			options.AddOption("protocol", true, "Specifies the connection protocol ('http' or 'tcp').");
			options.AddOption("format", true, "Format used to serialize messages to/from the manager " +
			                                  "service ('xml', 'json' or 'binary')");
			options.AddOption("password", true, "The challenge password used in all connection handshaking " +
			                                    "throughout the network.");
			options.AddOption("user", true, "The name of the user to authenticate in a HTTP connection.");
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			string protocol = commandLine.GetOptionValue("protocol", "tcp");
			string address = commandLine.GetOptionValue("address", null);
			string format = commandLine.GetOptionValue("format", "binary");

			if (String.IsNullOrEmpty(address))
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

			NetworkConfigSource configSource = new NetworkConfigSource();
			configSource.AddNetworkNode(address);
			networkProfile.Configuration = configSource;

			((CloudAdmin) Application).SetNetworkContext(new NetworkContext(networkProfile));
			return true;
		}

		public override string Name {
			get { return "connect"; }
		}
	}
}