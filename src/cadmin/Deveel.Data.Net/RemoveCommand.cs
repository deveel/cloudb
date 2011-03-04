using System;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class RemoveCommand : Command {
		public override string Name {
			get { return "remove"; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		public override string[] Synopsis {
			get { return new string[] { "remove path <path-name> [ from <root> ]" }; }
		}
		
		private CommandResultCode RemovePath(NetworkContext context, string pathName, String rootAddress) {
			IServiceAddress address;
			// If machine is null, we need to find the machine the path is on,
			if (String.IsNullOrEmpty(rootAddress)) {
				PathInfo pathInfo = context.Network.GetPathInfo(pathName);
				if (pathInfo == null) {
					Error.WriteLine("path " + pathName + " was not found in the network");
					return CommandResultCode.ExecutionFailed;
				}

				address = pathInfo.RootLeader;
			} else {
				address = ServiceAddresses.ParseString(rootAddress);
			}
			
			Out.WriteLine("removing path " + pathName + " from root " + address);

			MachineProfile p = context.Network.GetMachineProfile(address);
			if (p == null) {
				Error.WriteLine("error: machine was not found in the network schema.");
				return CommandResultCode.ExecutionFailed;
			}
			if (!p.IsRoot) {
				Error.WriteLine("error: given machine is not a root.");
				return CommandResultCode.ExecutionFailed;
			}

			// Remove the path,
			context.Network.RemovePath(pathName, address);
			Out.WriteLine("done.");
			return CommandResultCode.Success;
		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			NetworkContext networkContext = (NetworkContext)context;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "path")
				return CommandResultCode.SyntaxError;
			
			string pathName = args.Current;
			string rootAddress = null;
			
			if (args.MoveNext()) {
				if (args.Current != "from")
					return CommandResultCode.SyntaxError;
				
				if (!args.MoveNext())
					return CommandResultCode.SyntaxError;
				
				rootAddress = args.Current;
			}
			
			return RemovePath(networkContext, pathName, rootAddress);
		}
	}
}