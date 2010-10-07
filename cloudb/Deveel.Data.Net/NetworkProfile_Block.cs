using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		public long GetBlockGuid(IServiceAddress block) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(block);
			// Check it's a block server,
			if (!machine_p.IsBlock)
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");

			RequestMessage request = new RequestMessage("serverGUID");
			request.Arguments.Add(block);

			ResponseMessage m = Command(block, ServiceType.Block, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the GUID
			return (long)m.Arguments[0].Value;
		}

		public long[] GetBlockList(IServiceAddress block) {
			InspectNetwork();

			MachineProfile machine_p = CheckMachineInNetwork(block);
			if (!machine_p.IsBlock)
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");

			RequestMessage request = new RequestMessage("blockSetReport");
			ResponseMessage m = Command(block, ServiceType.Block, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the block list,
			return (long[])m.Arguments[1].Value;
		}

		public void ProcessSendBlock(long block_id, IServiceAddress source_block_server, IServiceAddress dest_block_server, long dest_server_sguid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			RequestMessage request = new RequestMessage("sendBlockTo");
			request.Arguments.Add(block_id);
			request.Arguments.Add(dest_block_server);
			request.Arguments.Add(dest_server_sguid);
			request.Arguments.Add(manager_server);

			ResponseMessage m = Command(source_block_server, ServiceType.Block, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}