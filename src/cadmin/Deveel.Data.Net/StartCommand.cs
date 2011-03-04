using System;
using System.Collections.Generic;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class StartCommand : Command {
		public override string Name {
			get { return "start"; }
		}
				
		public override string[] Synopsis {
			get { return new string[] { "start <manager|root|block> [on <service>]" } ; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		private CommandResultCode StartRole(NetworkContext context, string role, IServiceAddress address) {			
			Out.WriteLine("Starting role " + role + " on " + address);

			MachineProfile p = context.Network.GetMachineProfile(address);
			if (p == null) {
				Error.WriteLine("Error: Machine was not found in the network schema.");
				return CommandResultCode.ExecutionFailed;
			}

			// Here we have some rules,
			// 1. There must be a manager service assigned before block and roots can be
			//    assigned.

			MachineProfile[] currentManagers = context.Network.ManagerServers;
			if (!role.Equals("manager") && (currentManagers == null || currentManagers.Length == 0)) {
				Error.WriteLine("Error: Can not assign block or root role when no manager is available on the network.");
				return CommandResultCode.ExecutionFailed;
			}

			// Check if the machine already performing the role,
			bool alreadyDoingIt = false;
			if (role.Equals("block")) {
				alreadyDoingIt = p.IsBlock;
			} else if (role.Equals("manager")) {
				alreadyDoingIt = p.IsManager;
			} else if (role.Equals("root")) {
				alreadyDoingIt = p.IsRoot;
			} else {
				Error.WriteLine("Unknown role " + role);
				return CommandResultCode.SyntaxError;
			}

			if (alreadyDoingIt) {
				Error.WriteLine("Error: The machine is already assigned to the " + role + " role.");
				return CommandResultCode.ExecutionFailed;
			}

			// Perform the assignment,
			if (role.Equals("block")) {
				context.Network.StartService(address, ServiceType.Block);
				context.Network.RegisterBlock(address);
			} else if (role.Equals("manager")) {
				context.Network.StartService(address, ServiceType.Manager);
				context.Network.RegisterManager(address);
			} else if (role.Equals("root")) {
				context.Network.StartService(address, ServiceType.Root);
				context.Network.RegisterRoot(address);
			} else {
				Error.WriteLine("Unknown role " + role);
				return CommandResultCode.SyntaxError;
			}
			
			Out.WriteLine("done.");
			return CommandResultCode.Success;
		}
		
		public override IEnumerator<string> Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			return base.Complete(dispatcher, partialCommand, lastWord);
		}
		
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			NetworkContext networkContext = context as NetworkContext;
			if (networkContext == null)
				return CommandResultCode.ExecutionFailed;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			string role = args.Current;
			
			IServiceAddress address = null;

			if (args.MoveNext()) {
				if (args.Current != "on")
					return CommandResultCode.SyntaxError;

				if (!args.MoveNext())
					return CommandResultCode.SyntaxError;

				try {
					address = ServiceAddresses.ParseString(args.Current);
				} catch(Exception) {
					Error.WriteLine("The address specified is invalid.");
					return CommandResultCode.ExecutionFailed;
				}
			} else {
				IServiceAddress[] addresses = networkContext.Network.Configuration.NetworkNodes;
				if (addresses != null && addresses.Length == 1)
					address = addresses[0];
			}

			if (address == null) {
				Error.WriteLine("unable to determine the address of the service to start.");
				return CommandResultCode.ExecutionFailed;
			}
			
			return StartRole(networkContext, role, address);
		}
	}
}