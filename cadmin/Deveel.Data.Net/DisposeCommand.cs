using System;
using System.Collections.Generic;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class DisposeCommand : Command {
		public override string Name {
			get { return "dispose"; }
		}
		
		public override string[] Aliases {
			get { return new string[] { "stop" } ; }
		}
		
		public override string[] Synopsis {
			get { return base.Synopsis; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		private CommandResultCode StopRole(NetworkContext context, string role, string machine) {
			IServiceAddress address = ServiceAddresses.ParseString(machine);
			
			Out.WriteLine("Stopping role " + role + " on " + address.ToString());

			MachineProfile p = context.Network.GetMachineProfile(address);
			if (p == null) {
				Out.WriteLine("Error: Machine was not found in the network schema.");
				return CommandResultCode.ExecutionFailed;
			}

			// Here we have some rules,
			// 1. The manager can not be relieved until all block and root servers have
			//    been.

			MachineProfile currentManager = context.Network.ManagerServer;
			MachineProfile[] currentRoots = context.Network.RootServers;
			MachineProfile[] currentBlocks = context.Network.BlockServers;
			if (role.Equals("manager")) {
				if (currentRoots.Length > 0 ||
					currentBlocks.Length > 0) {
					Error.WriteLine("Error: Can not relieve manager role when there are existing block and root assignments.");
					return CommandResultCode.ExecutionFailed;
				}
			}

			// Check that the machine is performing the role,
			bool isPerforming = false;
			if (role.Equals("block")) {
				isPerforming = p.IsBlock;
			} else if (role.Equals("manager")) {
				isPerforming = p.IsManager;
			} else if (role.Equals("root")) {
				isPerforming = p.IsRoot;
			} else {
				Error.WriteLine("Unknown role " + role);
				return CommandResultCode.SyntaxError;
			}

			if (!isPerforming) {
				Error.WriteLine("Error: The machine is not assigned to the " + role + " role.");
				return CommandResultCode.ExecutionFailed;
			}

			// Perform the assignment,
			if (role.Equals("block")) {
				context.Network.DeregisterBlock(address);
				context.Network.StopService(address, ServiceType.Block);
			} else if (role.Equals("manager")) {
				context.Network.StopService(address, ServiceType.Manager);
			} else if (role.Equals("root")) {
				context.Network.DeregisterRoot(address);
				context.Network.StopService(address, ServiceType.Root);
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
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			string address = args.Current;
			
			return StopRole(networkContext, role, address);
		}
	}
}