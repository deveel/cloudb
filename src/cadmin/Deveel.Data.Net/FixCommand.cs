using System;
using System.Collections.Generic;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	internal class FixCommand : Command {
		public override bool RequiresContext {
			get { return true; }
		}

		private void BlockMapProcess(NetworkContext context, IBlockMapProcess process) {
			Out.WriteLine("Processing...");
			
			// Refresh
			context.Network.Refresh();

			// Generate a map of server guid value to MachineProfile for that machine
			// node currently available.
			MachineProfile[] blockServers = context.Network.BlockServers;
			long[] availableBlockGuids = new long[blockServers.Length];
			Dictionary<long, MachineProfile> sguidToAddress = new Dictionary<long, MachineProfile>();
			for (int i = 0; i < blockServers.Length; ++i) {
				long serverGuid = context.Network.GetBlockGuid(blockServers[i].Address);
				availableBlockGuids[i] = serverGuid;
				sguidToAddress[availableBlockGuids[i]] = blockServers[i];
			}

			Array.Sort(availableBlockGuids);

			int blockServerCount = blockServers.Length;
			Out.WriteLine("Block servers currently available: " + blockServerCount);
			if (blockServerCount < 3) {
				Out.WriteLine("WARNING: There are currently less than 3 block servers available.");
			}

			// The map of block_id to list of server guids that contain the block,
			Dictionary<long, List<long>> blockIdMap = new Dictionary<long, List<long>>();

			// For each block server,
			Dictionary<long, MachineProfile>.KeyCollection serverGuids = sguidToAddress.Keys;
			foreach (long serverGuid in serverGuids) {
				MachineProfile block = sguidToAddress[serverGuid];
				// Fetch the list of blocks for the server,
				long[] blockIds = context.Network.GetBlockList(block.Address);
				foreach (long blockId in blockIds) {
					// Build the association,
					List<long> list = blockIdMap[blockId];
					if (list == null) {
						list = new List<long>(5);
						blockIdMap[blockId] = list;
					}
					list.Add(serverGuid);
				}
			}

			// Now, 'block_id_map' contains the actual map of block id to servers as
			// reported by the block servers. We now need to compare this to the map
			// the manager server has.

			// The total number of mappings recorded by the manager,
			long count = context.Network.GetBlockMappingCount();
			long[] m = context.Network.GetBlockMappingRange(0, Int64.MaxValue);

			long blCur = -1;
			List<long> list1 = new List<long>();

			for (int i = 0; i < m.Length; i += 2) {
				long bl = m[i];      // the block id
				long sg = m[i + 1];  // the server guid

				if (bl != blCur) {
					if (list1.Count > 0) {
						// Check this block,
						List<long> in_list = blockIdMap[blCur];
						process.ManagerProcess(blCur, list1, in_list, sguidToAddress);
					}
					list1.Clear();
					blCur = bl;
				}
				list1.Add(sg);
			}

			// For each block,
			Dictionary<long, List<long>>.KeyCollection blockIds1 = blockIdMap.Keys;
			int minBlockThreshold = Math.Min(3, Math.Max(blockServerCount, 1));
			foreach (long block_id in blockIds1) {
				List<long> blockIdOnSguids = blockIdMap[block_id];
				List<long> availableSguids = new List<long>();
				foreach (long block_sguid in blockIdOnSguids) {
					if (Array.BinarySearch(availableBlockGuids, block_sguid) >= 0) {
						availableSguids.Add(block_sguid);
					}
				}

				process.Process(block_id, availableSguids,
								availableBlockGuids, minBlockThreshold,
								sguidToAddress);

			}

		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			NetworkContext networkContext = (NetworkContext) context;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "block")
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "availability" &&
				args.Current != "avail")
				return CommandResultCode.SyntaxError;

			try {
				Random r = new Random();
				BlockMapProcess(networkContext, new BlockMapProcessImpl(this, networkContext, r));

			} catch(Exception e) {
				Error.WriteLine("unable to fix block availability: " + e.Message);
				return CommandResultCode.ExecutionFailed;
			}

			return CommandResultCode.Success;
		}

		public override string Name {
			get { return "fix"; }
		}

		#region BlockMapProcessImpl

		private sealed class BlockMapProcessImpl : IBlockMapProcess {
			public BlockMapProcessImpl(FixCommand command, NetworkContext context, Random r) {
				this.command = command;
				this.context = context;
				this.r = r;
			}

			private readonly Random r;
			private readonly FixCommand command;
			private readonly NetworkContext context;

			public void ManagerProcess(long blockId, IList<long> managerSguids, IList<long> actualSguids, IDictionary<long, MachineProfile> sguid_to_address) {
				if (actualSguids == null) {
					// This means this block_id is referenced by the manager but there
					// are no blocks available that currently store it.

					// ISSUE: This is fairly normal and will happen when the manager
					//   server is restarted.

				} else {
					foreach (long sguid in managerSguids) {
						if (!actualSguids.Contains(sguid)) {
							// Manager has a block_id association to a block server that
							// doesn't hold the record. The association that should be
							// removed is 'block_id -> sguid'

							command.Out.WriteLine("Removing block association: " + blockId + " -> " + sguid_to_address[sguid].Address);
							context.Network.RemoveBlockAssociation(blockId, sguid);

						}
					}
					foreach (long sguid in actualSguids) {
						if (!managerSguids.Contains(sguid)) {
							// Manager doesn't have a block_id association that it should
							// have. The association made is 'block_id -> sguid'

							command.Out.WriteLine("Adding block association: " + blockId + " -> " + sguid_to_address[sguid].Address);
							context.Network.AddBlockAssociation(blockId, sguid);
						}
					}
				}
			}

			public void Process(long block_id, IList<long> available_sguids_containing_block_id, long[] available_block_servers, long min_threshold, IDictionary<long, MachineProfile> sguid_to_address) {
				int block_availability = available_sguids_containing_block_id.Count;
				// If a block has 0 availability,
				if (block_availability == 0) {
					command.Error.WriteLine("ERROR: Block " + block_id + " currently has 0 availability!");
					command.Error.WriteLine("  I can't fix this - A block server containing a copy of this block needs to be added on the network.");
				} else if (block_availability < min_threshold) {
					// The set of all block servers we can copy the block to,
					List<long> dest_block_servers = new List<long>(available_block_servers.Length);
					// Of all the block servers, find the list of servers that don't
					// contain the block_id
					foreach (long available_server in available_block_servers) {
						bool use_server = true;
						foreach (long server in available_sguids_containing_block_id) {
							if (server == available_server) {
								use_server = false;
								break;
							}
						}
						if (use_server) {
							dest_block_servers.Add(available_server);
						}
					}

					long source_sguid = available_sguids_containing_block_id[0];
					MachineProfile source_server = sguid_to_address[source_sguid];

					// Pick servers to bring the availability of the block to the ideal
					// value of 3.

					int ideal_count = (int)(min_threshold - block_availability);
					for (int i = 0; i < ideal_count; ++i) {
						int lid = r.Next(dest_block_servers.Count);
						long dest_server_sguid = dest_block_servers[lid];
						dest_block_servers.Remove(lid);
						MachineProfile dest_server = sguid_to_address[dest_server_sguid];

						command.Out.Write("Copying ");
						command.Out.Write(block_id);
						command.Out.Write(" from ");
						command.Out.Write(source_server.Address);
						command.Out.Write(" to ");
						command.Out.Write(dest_server.Address);
						command.Out.WriteLine(".");

						// Send the command to copy,
						context.Network.ProcessSendBlock(block_id, source_server.Address, dest_server.Address, dest_server_sguid);
					}

					command.Out.WriteLine("block " + block_id + " can be copied to: " + dest_block_servers);
				}
			}
		}

		#endregion
	}
}