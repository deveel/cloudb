using System;
using System.Collections.Generic;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class InitializeCommand : Command {
		public override string Name {
			get { return "initialize"; }
		}
		
		public override string[] Aliases {
			get { return new string[] { "start" }; }
		}
		
		public override string[] Synopsis {
			get { return new string[] { "initialize manager on <address>", 
					"initialize root on <address>",
				"initialize block on <address>" } ; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		private CommandResultCode StartRole(NetworkContext context, string role, string machine) {
			IServiceAddress address = ServiceAddresses.ParseString(machine);
			
			Out.WriteLine("Starting role " + role + " on " + address.ToString());

			MachineProfile p = context.Network.GetMachineProfile(address);
			if (p == null) {
				Error.WriteLine("Error: Machine was not found in the network schema.");
				return CommandResultCode.ExecutionFailed;
			}

			// Here we have some rules,
			// 1. There must be a manager service assigned before block and roots can be
			//    assigned.
			// 2. Only one manager server can exist.

			MachineProfile current_manager = context.Network.ManagerServer;
			if (!role.Equals("manager") && current_manager == null) {
				Error.WriteLine("Error: Can not assign block or root role when no manager is available on the network.");
				return CommandResultCode.ExecutionFailed;
			} else if (current_manager != null) {
				Out.WriteLine("Error: Can not assign manager because manager role already assigned.");
				return CommandResultCode.ExecutionFailed;
			}

			// Check if the machine already performing the role,
			bool already_doing_it = false;
			if (role.Equals("block")) {
				already_doing_it = p.IsBlock;
			} else if (role.Equals("manager")) {
				already_doing_it = p.IsManager;
			} else if (role.Equals("root")) {
				already_doing_it = p.IsRoot;
			} else {
				Error.WriteLine("Unknown role " + role);
				return CommandResultCode.SyntaxError;
			}

			if (already_doing_it) {
				Error.WriteLine("Error: The machine is already assigned to the " + role + " role.");
				return CommandResultCode.ExecutionFailed;
			}

			// Perform the assignment,
			if (role.Equals("block")) {
				context.Network.StartService(address, ServiceType.Block);
				context.Network.RegisterBlock(address);
			} else if (role.Equals("manager")) {
				context.Network.StartService(address, ServiceType.Manager);
			} else if (role.Equals("root")) {
				context.Network.StartService(address, ServiceType.Root);
				context.Network.RegisterBlock(address);
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
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			if (args.Current != "on")
				return CommandResultCode.SyntaxError;
			
			string address = args.Current;
			return StartRole(networkContext, role, address);
		}
	}
}