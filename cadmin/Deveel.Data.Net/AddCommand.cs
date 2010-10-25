using System;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class AddCommand : Command {
		public override string Name {
			get { return "add"; }
		}
		
		public override string[] Synopsis {
			get { return new string[] { "add path <type> <path-name> to <root>", 
					"add base path <path-name> to <root>" }; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		private CommandResultCode AddPath(NetworkContext context, string pathType, string pathName, string rootAddress) {
			IServiceAddress address = ServiceAddresses.ParseString(rootAddress);
			Out.WriteLine("Adding path " + pathType + " " + pathName + " to root " + address.ToString());

			MachineProfile p = context.Network.GetMachineProfile(address);
			if (p == null) {
				Error.WriteLine("error: Machine was not found in the network schema.");
				return CommandResultCode.ExecutionFailed;
			}
			if (!p.IsRoot) {
				Error.WriteLine("error: Given machine is not a root.");
				return CommandResultCode.ExecutionFailed;
			}

			// Add the path,
			try {
				context.Network.AddPath(address, pathName, pathType);
			} catch (Exception e) {
				Error.WriteLine("cannot add the path: " + e.Message);
				return CommandResultCode.ExecutionFailed;
			}
			
			Out.WriteLine("done.");
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