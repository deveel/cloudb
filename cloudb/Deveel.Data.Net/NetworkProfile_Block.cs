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

			MessageRequest request = new MessageRequest("serverGUID");
			request.Arguments.Add(block);
			Message m = Command(block, ServiceType.Block, request);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));

			// Return the GUID
			return m.Arguments[0].ToInt64();
		}

		public long[] GetBlockList(IServiceAddress block) {
			InspectNetwork();

			MachineProfile machine_p = CheckMachineInNetwork(block);
			if (!machine_p.IsBlock)
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");

			MessageRequest msg_out = new MessageRequest();
			Message m = Command(block, ServiceType.Block, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));

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

			MessageRequest msg_out = new MessageRequest();
			msg_out.Arguments.Add(block_id);
			msg_out.Arguments.Add(dest_block_server);
			msg_out.Arguments.Add(dest_server_sguid);
			msg_out.Arguments.Add(manager_server);

			Message m = Command(source_block_server, ServiceType.Block, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));
		}
	}
}