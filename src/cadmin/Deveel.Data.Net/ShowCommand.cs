using System;
using System.Collections.Generic;
using Deveel.Collections;
using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	sealed class ShowCommand : Command {
		public override string Name {
			get { return "show"; }
		}
		
		public override string[] Synopsis {
			get { return new string[] { "show network", "show analytics", 
					"show paths", "show free",
				"show status" } ; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		private void ShowNetwork(NetworkContext context) {
			MachineProfile[] profiles = context.Network.MachineProfiles;

			int managerCount = 0;
			int rootCount = 0;
			int blockCount = 0;
			
			ColumnDesign[] columns = new ColumnDesign[2];
			columns[0] = new ColumnDesign("MRB");
			columns[0].Width = 20;
			columns[1] = new ColumnDesign("Address");
			
			TableRenderer table = new TableRenderer(columns, Out);
			context.Network.Refresh();

			foreach (MachineProfile p in profiles) {
				if (p.HasError) {
					ColumnValue[] row = new ColumnValue[2];
					row[0] = new ColumnValue(p.ErrorState);
					row[1] = new ColumnValue(p.Address.ToString());
					table.AddRow(row);
				} else {
					string mrb = String.Empty;
					mrb += p.IsManager ? "M" : ".";
					mrb += p.IsRoot ? "R" : ".";
					mrb += p.IsBlock ? "B" : ".";
					
					ColumnValue[] row = new ColumnValue[2];
					row[0] = new ColumnValue(mrb);
					row[1] = new ColumnValue(p.Address.ToString());
					table.AddRow(row);

					managerCount += p.IsManager ? 1 : 0;
					rootCount += p.IsRoot ? 1 : 0;
					blockCount += p.IsBlock ? 1 : 0;
				}
			}
			
			table.CloseTable();
			
			Out.WriteLine();
			if (profiles.Length == 1) {
				Out.WriteLine("one machine in the network.");
			} else {
				Out.WriteLine(profiles.Length + " machines in the network.");
			}
			
			if (managerCount == 0) {
				Out.Write("none manager");
			} else if (managerCount == 1) {
				Out.Write("one manager");
			} else {
				Out.Write(managerCount + " managers");
			}
			
			Out.Write(", ");
			
			if (rootCount == 0) {
				Out.Write("none root");
			} else if (rootCount == 1) {
				Out.Write("one root");
			} else {
				Out.Write(rootCount + " roots");
			}
			
			Out.Write(", ");
			
			if (blockCount == 0) {
				Out.Write("none block");
			} else if (blockCount == 1) {
				Out.Write("one block");
			} else {
				Out.Write(blockCount + " blocks");
			}
			
			Out.WriteLine();
		}
				
		private CommandResultCode ShowPaths(NetworkContext context) {
			MachineProfile[] roots = context.Network.RootServers;
			if (roots.Length == 0) {
				Error.WriteLine("No root servers available on the network.");
				return CommandResultCode.ExecutionFailed;
			}

			context.Network.Refresh();

			// For each root server in the network,
			foreach (MachineProfile root in roots) {
				IServiceAddress rootService = root.Address;
				Out.Write("Root server: ");
				Out.WriteLine(rootService.ToString());
				Out.WriteLine();
				
				PathProfile[] paths = context.Network.GetPathsFromRoot(rootService);

				if (paths.Length == 0) {
					Out.WriteLine("  [No paths on this root server]");
					Out.WriteLine();
				} else {
					ColumnDesign[] columns = new ColumnDesign[3];
					columns[0] = new ColumnDesign("Name", ColumnAlignment.Right);
					columns[1] = new ColumnDesign("Type", ColumnAlignment.Center);
					columns[2] = new ColumnDesign("Status", ColumnAlignment.Center);
					
					TableRenderer table = new TableRenderer(columns, Out);
					foreach (PathProfile p in paths) {
						ColumnValue[] row = new ColumnValue[3];
						row[0] = new ColumnValue(p.Path);
						row[1] = new ColumnValue(p.PathType);
						
						try {
							string stats = context.Network.GetPathStats(p.RootAddress, p.Path);
							if (stats != null)
								row[2] = new ColumnValue(stats);
						} catch (NetworkAdminException e) {
							row[2] = new ColumnValue("ERROR RETRIEVING");
						}
					}
					
					table.CloseTable();
				}
			}

			return CommandResultCode.Success;
		}

		public void ShowStatus(NetworkContext context) {
			ColumnDesign[] columns = new ColumnDesign[3];
			columns[0] = new ColumnDesign("Status");
			columns[1] = new ColumnDesign("Service");
			columns[2] = new ColumnDesign("Address");
			
			TableRenderer table = new TableRenderer(columns, Out);
			
			context.Network.Refresh();

			IDictionary<IServiceAddress, string> status_info = null;
			// Manager servers status,
			MachineProfile manager = context.Network.ManagerServer;
			if (manager != null) {
				ColumnValue[] row = new ColumnValue[3];
				row[0] = new ColumnValue("UP");
				row[1] = new ColumnValue("Manager");
				row[2] = new ColumnValue(manager.Address.ToString());

				try {
					status_info = context.Network.GetBlocksStatus();
				} catch (NetworkAdminException e) {
					Error.WriteLine("Error retrieving manager status info: " + e.Message);
				}
				
				table.AddRow(row);
			} else {
				Error.WriteLine("! Manager server not available");
			}

			// Status of root servers
			MachineProfile[] roots = context.Network.RootServers;
			if (roots.Length == 0) {
				Out.WriteLine("! Root servers not available");
			}
			foreach (MachineProfile r in roots) {
				ColumnValue[] row = new ColumnValue[3];
				if (r.HasError) {
					row[0] = new ColumnValue("DOWN");
				} else {
					row[0] = new ColumnValue("UP");
				}
				
				row[1] = new ColumnValue("Root");
				row[2] = new ColumnValue(r.Address.ToString());
				
				if (r.HasError) {
					Out.Write("  ");
					Out.WriteLine(r.ErrorState);
				}

				table.AddRow(row);
			}

			// The block servers we fetch from the map,
			List<IServiceAddress> blocks = new List<IServiceAddress>();
			if (status_info != null) {
				foreach (IServiceAddress s in status_info.Keys) {
					blocks.Add(s);
				}
			} else {
				MachineProfile[] sblocks = context.Network.BlockServers;
				foreach (MachineProfile b in sblocks) {
					blocks.Add(b.Address);
				}
			}
			blocks.Sort();

			if (blocks.Count == 0) {
				Out.WriteLine("! Block servers not available");
			}
			foreach (IServiceAddress b in blocks) {
				ColumnValue[] row = new ColumnValue[3];
				
				if (status_info != null) {
					String status_str = status_info[b];
					if (status_str.Equals("UP")) {
						// Manager reported up
						row[0] = new ColumnValue("UP");
					} else if (status_str.Equals("DOWN CLIENT REPORT")) {
						// Manager reported down from client report of error
						row[0] = new ColumnValue("D-CR");
					} else if (status_str.Equals("DOWN HEARTBEAT")) {
						// Manager reported down from heart beat check on the server
						row[0] = new ColumnValue("D-HB");
					} else if (status_str.Equals("DOWN SHUTDOWN")) {
						// Manager reported down from shut down request
						row[0] = new ColumnValue("D-SD");
					} else {
						row[0] = new ColumnValue("?ERR");
					}

				} else {
					// Try and get status from machine profile
					MachineProfile r = context.Network.GetMachineProfile(b);
					if (r.HasError) {
						row[0] = new ColumnValue("DOWN");
					} else {
						row[0] = new ColumnValue("UP");
					}
				}

				row[1] = new ColumnValue("Block");
				row[2] = new ColumnValue(b.ToString());
				
				table.AddRow(row);
			}

			table.CloseTable();
		}

		
		public override IEnumerator<string> Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			IEnumerator<string> complete = null;
			
			SortedList<string, string> commands = new SortedList<string, string>();
			commands.Add("analytics", "");
			commands.Add("free", "");
			commands.Add("network", "");
			commands.Add("paths", "");
			commands.Add("status", "");
			
			string[] sp = partialCommand.Trim().Split(' ');
			if (sp.Length >= 1 && String.Compare(sp[0], "show", true) == 0) {
				if (lastWord.Length > 0) {
					complete = SubsetCollection<string>.Tail(commands, lastWord).GetEnumerator();
				} else {
					complete = commands.Keys.GetEnumerator();
				}
			}
			
			return complete;
		}
		
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (context == null)
				return CommandResultCode.ExecutionFailed;
			
			NetworkContext networkContext = (NetworkContext)context;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			if (args.Current == "analytics") {
				
			} else if (args.Current == "free") {
				
			} else if (args.Current == "network") {
				ShowNetwork(networkContext);
				return CommandResultCode.Success;
			} else if (args.Current == "paths") {
				return ShowPaths(networkContext);
			} else if (args.Current == "status") {
				ShowStatus(networkContext);
				return CommandResultCode.Success;
			} else {
				return CommandResultCode.SyntaxError;
			}
			
			return CommandResultCode.ExecutionFailed;
		}
	}
}