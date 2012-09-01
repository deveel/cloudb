using System;
using System.Collections.Generic;
using System.Text;

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
			int managerCount = 0;
			int rootCount = 0;
			int blockCount = 0;
			
			ColumnDesign[] columns = new ColumnDesign[2];
			columns[0] = new ColumnDesign("MRB");
			columns[0].Width = 20;
			columns[1] = new ColumnDesign("Address");
			
			TableRenderer table = new TableRenderer(columns, Out);
			context.Network.Refresh();

			MachineProfile[] profiles = context.Network.GetAllMachineProfiles();

			foreach (MachineProfile p in profiles) {
				if (p.IsError) {
					ColumnValue[] row = new ColumnValue[2];
					row[0] = new ColumnValue(p.ErrorMessage);
					row[1] = new ColumnValue(p.ServiceAddress.ToString());
					table.AddRow(row);
				} else {
					string mrb = String.Empty;
					mrb += p.IsManager ? "M" : ".";
					mrb += p.IsRoot ? "R" : ".";
					mrb += p.IsBlock ? "B" : ".";
					
					ColumnValue[] row = new ColumnValue[2];
					row[0] = new ColumnValue(mrb);
					row[1] = new ColumnValue(p.ServiceAddress.ToString());
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

		private CommandResultCode ShowAnalytics(NetworkContext context) {
			 context.Network.Refresh();

			MachineProfile[] profiles = context.Network.GetAllMachineProfiles();

			foreach (MachineProfile p in profiles) {
				Out.WriteLine(p.ServiceAddress.ToString());
				Out.Write("  ");
				if (p.IsError) {
					Out.Write("Error: ");
					Out.WriteLine(p.ErrorMessage);
					return CommandResultCode.ExecutionFailed;
				}

				long[] stats = context.Network.GetAnalyticsStats(p.ServiceAddress);
				if (stats.Length < 4) {
					Out.WriteLine("Sorry, no analytics available yet.");
					return CommandResultCode.ExecutionFailed;
				}

				PrintStatItem(stats, 1);
				Out.Write(" ");
				PrintStatItem(stats, 5);
				Out.Write(" ");
				PrintStatItem(stats, 15);
				Out.WriteLine();

				Out.Flush();
			}

			Out.WriteLine();
			return CommandResultCode.Success;
		}

		public void PrintStatItem(long[] stats, int itemCount) {
			long opCount = 0;
			long opTime = 0;
			int c = 0;
			for (int i = stats.Length - 4; i >= 0 && c < itemCount; i -= 4) {
				opTime += stats[i + 2];
				opCount += stats[i + 3];
				++c;
			}

			if (opCount != 0) {
				double avg = Math.Round(opTime/(double)opCount, MidpointRounding.ToEven);

				Out.Write(opCount);
				Out.Write("(" + avg + " ms)");
			} else {
				Out.Write("0(0 ms)");
			}
		}


		private void OutputPathInfo(NetworkContext context, PathInfo p) {
			string pathName = p.PathName;

			Out.Write("+Name: ");
			Out.Write(pathName);
			Out.Write(" (");
			Out.Write(p.PathType);
			Out.WriteLine(")");

			Out.Write(" Srvs: ");
			IServiceAddress leader = p.RootLeader;
			IServiceAddress[] srvs = p.RootServers;
			foreach (IServiceAddress srv in srvs) {
				bool il = srv.Equals(leader);
				if (il)
					Out.Write("[");
				Out.Write(srv.ToString());
				if (il)
					Out.Write("*]");
				Out.Write(" ");
			}
			Out.WriteLine();

			Out.Write(" Status: ");
			try {
				String stats = context.Network.GetPathStats(p);
				if (stats != null) {
					Out.Write(stats);
				}
			} catch (NetworkAdminException e) {
				Out.Write("Error retrieving stats: " + e.Message);
			}

			Out.WriteLine();
			Out.WriteLine();
		}

		private CommandResultCode ShowPaths(NetworkContext context) {
			MachineProfile[] roots = context.Network.GetRootServers();
			if (roots.Length == 0) {
				Out.WriteLine("No root servers available on the network.");
				return CommandResultCode.ExecutionFailed;
			}

			Out.Flush();
			context.Network.Refresh();

			// Get all paths from the manager cluster,
			String[] pathNames = context.Network.GetAllPathNames();

			int count = 0;
			foreach (string pathName in pathNames) {
				PathInfo pathInfo = context.Network.GetPathInfo(pathName);
				OutputPathInfo(context, pathInfo);
				Out.Flush();
				++count;
			}

			Out.WriteLine();
			Out.WriteLine("Path Count: " + count);
			Out.Flush();

			return CommandResultCode.Success;
		}

		private void ShowStatus(NetworkContext context) {
			ColumnDesign[] columns = new ColumnDesign[3];
			columns[0] = new ColumnDesign("Status");
			columns[1] = new ColumnDesign("Service");
			columns[2] = new ColumnDesign("Address");
			
			TableRenderer table = new TableRenderer(columns, Out);
			
			context.Network.Refresh();

			IDictionary<IServiceAddress, ServiceStatus> statusInfo = null;
			// Manager servers status,
			MachineProfile[] managers = context.Network.GetManagerServers();
			if (managers.Length > 0) {
				foreach (var manager in managers) {
					ColumnValue[] row = new ColumnValue[3];
					row[0] = new ColumnValue("UP");
					row[1] = new ColumnValue("Manager");
					row[2] = new ColumnValue(manager.ServiceAddress.ToString());

					try {
						statusInfo = context.Network.GetBlocksStatus();
					} catch (NetworkAdminException e) {
						Error.WriteLine("Error retrieving manager status info: " + e.Message);
					}

					table.AddRow(row);
				}
			} else {
				Error.WriteLine("! Manager server not available");
			}

			// Status of root servers
			MachineProfile[] roots = context.Network.GetRootServers();
			if (roots.Length == 0) {
				Out.WriteLine("! Root servers not available");
			}
			foreach (MachineProfile r in roots) {
				ColumnValue[] row = new ColumnValue[3];
				if (r.IsError) {
					row[0] = new ColumnValue("DOWN");
				} else {
					row[0] = new ColumnValue("UP");
				}
				
				row[1] = new ColumnValue("Root");
				row[2] = new ColumnValue(r.ServiceAddress.ToString());
				
				if (r.IsError) {
					Out.Write("  ");
					Out.WriteLine(r.ErrorMessage);
				}

				table.AddRow(row);
			}

			// The block servers we fetch from the map,
			List<IServiceAddress> blocks = new List<IServiceAddress>();
			if (statusInfo != null) {
				foreach (IServiceAddress s in statusInfo.Keys) {
					blocks.Add(s);
				}
			} else {
				MachineProfile[] sblocks = context.Network.GetBlockServers();
				foreach (MachineProfile b in sblocks) {
					blocks.Add(b.ServiceAddress);
				}
			}

			blocks.Sort();

			if (blocks.Count == 0) {
				Out.WriteLine("! Block servers not available");
			}

			foreach (IServiceAddress b in blocks) {
				ColumnValue[] row = new ColumnValue[3];
				
				if (statusInfo != null) {
					ServiceStatus status = statusInfo[b];
					if (status == ServiceStatus.Up) {
						// Manager reported up
						row[0] = new ColumnValue("UP");
					} else if (status == ServiceStatus.DownClientReport) {
						// Manager reported down from client report of error
						row[0] = new ColumnValue("D-CR");
					} else if (status == ServiceStatus.DownHeartbeat) {
						// Manager reported down from heart beat check on the server
						row[0] = new ColumnValue("D-HB");
					} else if (status == ServiceStatus.DownShutdown) {
						// Manager reported down from shut down request
						row[0] = new ColumnValue("D-SD");
					} else {
						row[0] = new ColumnValue("?ERR");
					}

				} else {
					// Try and get status from machine profile
					MachineProfile r = context.Network.GetMachineProfile(b);
					if (r.IsError) {
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

		private void ShowFree(NetworkContext context) {
			// Refresh
			context.Network.Refresh();

			MachineProfile[] machines = context.Network.GetAllMachineProfiles();
			if (machines.Length == 0) {
				Out.WriteLine("No machines in the network.");
			} else {
				ColumnDesign[] columns = new ColumnDesign[4];
				columns[0] = new ColumnDesign("Machine");
				columns[1] = new ColumnDesign("Used Memory");
				columns[2] = new ColumnDesign("Used Disk");
				columns[3] = new ColumnDesign("Notes");

				TableRenderer table = new TableRenderer(columns, Out);

				foreach (var machine in machines) {
					ColumnValue[] row = new ColumnValue[4];
					if (machine.IsError) {
						row[3] = new ColumnValue(" ERROR: " + machine.ErrorMessage);
					} else {
						row[0] = new ColumnValue(machine.ServiceAddress.ToString());
						row[1] = new ColumnValue(MemoryReport(machine.MemoryUsed, machine.MemoryTotal));
						row[2] = new ColumnValue(MemoryReport(machine.DiskUsed, machine.DiskTotal));
						if (machine.DiskUsed > ((double) machine.DiskTotal*0.85d)) {
							row[3] = new ColumnValue(" WARNING: Node is close to full - used storage within 85% of total");
						}
					}

					table.AddRow(row);
				}

				table.CloseTable();
			}
		}

		private static String MemoryReport(long used, long total) {
			StringBuilder b = new StringBuilder();

			String sz;
			double precision;

			if (total >= (1024L * 1024L * 1024L * 1024L)) {
				sz = " TB";
				precision = (1024L * 1024L * 1024L * 1024L);
			} else if (total >= (1024L * 1024L * 1024L)) {
				sz = " GB";
				precision = (1024L * 1024L * 1024L);
			} else {
				sz = " MB";
				precision = (1024L * 1024L);
			}

			double totalMb = ((double)total) / precision;
			double usedMb = ((double)used) / precision;
			double showt = Math.Round(totalMb, MidpointRounding.ToEven);
			double showu = Math.Round(usedMb, MidpointRounding.ToEven);

			b.Append(showu.ToString());
			b.Append("/");
			b.Append(showt.ToString());
			b.Append(sz);

			return b.ToString();
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
			if (sp.Length >= 1 && String.Compare(sp[0], "show", StringComparison.OrdinalIgnoreCase) == 0) {
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

			if (args.Current == "analytics")
				return ShowAnalytics(networkContext);
			if (args.Current == "free") {
				ShowFree(networkContext);
				return CommandResultCode.Success;
			}
			if (args.Current == "network") {
				ShowNetwork(networkContext);
				return CommandResultCode.Success;
			} 
			if (args.Current == "paths") {
				return ShowPaths(networkContext);
			} 
			if (args.Current == "status") {
				ShowStatus(networkContext);
				return CommandResultCode.Success;
			}

			return CommandResultCode.SyntaxError;
		}
	}
}