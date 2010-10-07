using System;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		public long GetBlockGuid(IServiceAddress block) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(block);
			// Check it's a block server,
			if (!machine_p.IsBlock)
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("serverGUID", new object[] { block }));

			Message m = Command(block, ServiceType.Block, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the GUID
			return (long)m[0];
		}

		public long[] GetBlockList(IServiceAddress block) {
			InspectNetwork();

			MachineProfile machine_p = CheckMachineInNetwork(block);
			if (!machine_p.IsBlock)
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("blockSetReport"));

			Message m = Command(block, ServiceType.Block, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the block list,
			return (long[])m[1];
		}

		public void ProcessSendBlock(long block_id, IServiceAddress source_block_server, IServiceAddress dest_block_server, long dest_server_sguid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(6);
			msg_out.AddMessage("sendBlockTo", block_id, dest_block_server, dest_server_sguid, manager_server);

			Message m = Command(source_block_server, ServiceType.Block, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}