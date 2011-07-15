using System;
using System.Collections.Generic;

using Deveel.Configuration;
using Deveel.Console;
using Deveel.Console.Commands;
using Deveel.Data.Net.Client;
using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net {
	internal class ConnectCommand : Command {
		private const string JsonSerializerTypeName = "Deveel.Data.Net.Client.JsonRpcMessageSerializer, cloudb-json";

		private static readonly IMessageSerializer JsonRpcMessageSerializer;

		static ConnectCommand() {
			Type jsonSerializerType = Type.GetType(JsonSerializerTypeName, false, true);
			if (jsonSerializerType != null) {
				try {
					JsonRpcMessageSerializer = (IMessageSerializer) Activator.CreateInstance(jsonSerializerType, true);
				} catch {
				}
			}
		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (Application.ActiveContext != null && Application.ActiveContext.IsIsolated) {
				Error.WriteLine("a context is already opened: try to disconnect first");
				Error.WriteLine();
				return CommandResultCode.ExecutionFailed;
			}
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			if (args.Current != "to")
				return CommandResultCode.SyntaxError;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string address = args.Current;
			IServiceAddress serviceAddress;

			try {
				serviceAddress = ServiceAddresses.ParseString(address);
			} catch(Exception) {
				Error.WriteLine("Invalid service address specified: {0}", address);
				return CommandResultCode.ExecutionFailed;
			}

			NetworkConfigSource configSource = new NetworkConfigSource();

			try {
				configSource.AddNetworkNode(serviceAddress);
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

			IServiceConnector connector;
			if (protocol == "tcp") {
				if (String.IsNullOrEmpty(credentials)) {
					while(String.IsNullOrEmpty(credentials = Readline.ReadPassword("password: "))) {
						Error.WriteLine("please provide a valid password...");
					}
					Out.WriteLine();
				}
				connector = new TcpServiceConnector(credentials);
			} else if (protocol == "http") {
				string userName = credentials;
				string password = null;
				int index = credentials.IndexOf(':');
				if (index != -1) {
					password = credentials.Substring(index + 1);
					userName = credentials.Substring(0, index);
				}
				
				if (String.IsNullOrEmpty(password)) {
					while(String.IsNullOrEmpty(password = Readline.ReadPassword("password: "))) {
						Error.WriteLine("please provide a valid password...");
					}
					
					Out.WriteLine();
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
				if (JsonRpcMessageSerializer == null) {
					Error.WriteLine("JSON serializer was not installed.");
					Error.WriteLine();
					return CommandResultCode.ExecutionFailed;
				}
				serializer = JsonRpcMessageSerializer;
			} else {
				return CommandResultCode.SyntaxError;
			}

			connector.MessageSerializer = serializer;

			NetworkProfile networkProfile = new NetworkProfile(connector);
			networkProfile.Configuration = configSource;
			
			//TODO: test the connection is correct ...
			
			((CloudAdmin)Application).SetNetworkContext(new NetworkContext(networkProfile));
			
			Out.WriteLine("connected successfully to {0}" , address);
			Out.WriteLine();
			
			return CommandResultCode.Success;
		}

		public override IEnumerator<string> Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			string[] sp = partialCommand.Trim().Split(' ');

			IEnumerator<string> complete = null;

			if (sp.Length >= 1) {
				List<string> list = new List<string>();

				string s = sp[sp.Length - 1].ToLower();
				if (s == "connect") {
					list.Add("to");
				} else if (s == "identified") {
					list.Add("by");
				} else if (s == "to") {
					//TODO: load the saved addresses ...
				} else if (s == "on") {
					list.Add("http");
					list.Add("tcp");
				} else if (s == "with") {
					list.Add("xml");
					list.Add("json");
					list.Add("binary");
				} else if (sp.Length == 3 && sp[1] == "to") {
					list.Add("with");
					list.Add("on");
				} else if (s == "http" || s == "tcp") {
					list.Add("with");
					list.Add("identified");
				} else if (s == "xml" || s == "json" || s == "binary") {
					list.Add("on");
					list.Add("identified");
				}

				complete = new Collections.SortedMatchEnumerator(lastWord, list, StringComparer.InvariantCultureIgnoreCase);
			}

			return complete;
		}

		public override void RegisterOptions(Options options) {
			options.AddOption("h", "address", true, "The address to a node of the network (typically a manager).");			
			options.AddOption("x", "protocol", true, "Specifies the connection protocol ('http' or 'tcp').");
			options.AddOption("f", "format", true, "Format used to serialize messages to/from the manager " +
			                                  "service ('xml', 'json' or 'binary')");
			options.AddOption("p", "password", true, "The challenge password used in all connection handshaking " +
			                                    "throughout the network.");
			options.AddOption("u", "user", true, "The name of the user to authenticate in a HTTP connection.");
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
				if (JsonRpcMessageSerializer == null)
					throw new ApplicationException("The JSON serializer was not installed.");
				serializer = JsonRpcMessageSerializer;
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