using System;
using System.Text;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class AddCommand : Command {
		public override string Name {
			get { return "add"; }
		}
		
		public override string[] Synopsis {
			get {
				return new string[] {
				                    	"add path <type> <path-name> to <root>",
				                    	"add base path <path-name> to <root>",
										"add value <value> into <table> with key <key> [to <path>]"
				                    };
			}
		}
		
		public override bool RequiresContext {
			get { return true; }
		}

		private CommandResultCode AddPath(NetworkContext context, string pathType, string pathName, string rootAddress) {
			IServiceAddress[] addresses;

			try {
				addresses = ParseMachineAddressList(rootAddress);
			} catch (FormatException) {
				return CommandResultCode.SyntaxError;
			}

			// Check no duplicates in the list,
			bool duplicateFound = false;
			for (int i = 0; i < addresses.Length; ++i) {
				for (int n = i + 1; n < addresses.Length; ++n) {
					if (addresses[i].Equals(addresses[n])) {
						duplicateFound = true;
					}
				}
			}

			if (duplicateFound) {
				Out.WriteLine("Error: Duplicate root server in definition");
				return CommandResultCode.SyntaxError;
			}


			Out.WriteLine("Adding path " + pathType + " " + pathName + ".");
			Out.Write("Path Info ");
			Out.Write("Leader: " + addresses[0]);
			Out.Write(" Replicas: ");
			for (int i = 1; i < addresses.Length; ++i) {
				Out.Write(addresses[i]);
				Out.Write(" ");
			}
			Out.WriteLine();
			Out.Flush();

			for (int i = 0; i < addresses.Length; ++i) {
				MachineProfile p = context.Network.GetMachineProfile(addresses[i]);
				if (p == null) {
					Out.WriteLine("Error: Machine was not found in the network schema.");
					return CommandResultCode.ExecutionFailed;
				}
				if (!p.IsRoot) {
					Out.WriteLine("Error: Given machine is not a root.");
					return CommandResultCode.ExecutionFailed;
				}
			}

			// Add the path,
			try {
				context.Network.AddPathToNetwork(pathName, pathType, addresses[0], addresses);
			} catch (Exception e) {
				Error.WriteLine("cannot add the path: " + e.Message);
				return CommandResultCode.ExecutionFailed;
			}

			Out.WriteLine("done.");
			return CommandResultCode.Success;
		}

		private IServiceAddress[] ParseMachineAddressList(string rootAddress) {
			String[] machines = rootAddress.Split(',');

			try {
				IServiceAddress[] services = new IServiceAddress[machines.Length];
				for (int i = 0; i < machines.Length; ++i) {
					services[i] = ServiceAddresses.ParseString(machines[i].Trim());
				}
				return services;
			} catch (FormatException e) {
				Out.WriteLine("Error parsing machine address: " + e.Message);
				throw;
			}

		}

		private CommandResultCode AddValue(NetworkContext context, CommandArguments args) {
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string value = args.Current;

			if (String.IsNullOrEmpty(value))
				return CommandResultCode.ExecutionFailed;

			if (value[0] == '\'') {
				bool endFound = false;
				StringBuilder sb = new StringBuilder();
				while (!endFound) {
					for (int i = 0; !String.IsNullOrEmpty(value) && i < value.Length; i++) {
						char c = value[i];
						if (c == '\'' && i > 0) {
							endFound = true;
							break;
						}

						sb.Append(c);
					}

					if (!endFound && args.MoveNext()) {
						sb.Append(' ');
						value = args.Current;
					}
				}

				value = sb.ToString();
			}

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "into")
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string tableName = args.Current;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "with")
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "key")
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string key = args.Current;

			string pathName = null;

			if (args.MoveNext()) {
				if (args.Current != "to")
					return CommandResultCode.SyntaxError;
				if (!args.MoveNext())
					return CommandResultCode.SyntaxError;

				pathName = args.Current;
			}

			try {
				context.AddValueToPath(pathName, tableName, key, value);
			} catch(Exception e) {
				Error.WriteLine("error while adding the value: " + e.Message);
				Error.WriteLine();
				return CommandResultCode.ExecutionFailed;
			}
			
			return CommandResultCode.Success;
		}

		
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			NetworkContext networkContext = (NetworkContext)context;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			string pathType;
			
			if (args.Current == "path") {
				if (!args.MoveNext())
					return CommandResultCode.SyntaxError;
				
				pathType = args.Current;
			} else if (args.Current == "base") {
				if (!args.MoveNext())
					return CommandResultCode.SyntaxError;
				if (args.Current != "path")
					return CommandResultCode.SyntaxError;

				pathType = "Deveel.Data.BasePath, cloudbase";
			} else if (args.Current == "value") {
				return AddValue(networkContext, args);
			} else {
				return CommandResultCode.SyntaxError;
			}
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			string pathName = args.Current;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			if (args.Current != "to")
				return CommandResultCode.SyntaxError;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			string rootAddress = args.Current;
			
			return AddPath(networkContext, pathType, pathName, rootAddress);
		}
	}
}