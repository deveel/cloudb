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

			Message m = Command(block, ServiceType.Block, request);
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
			Message m = Command(block, ServiceType.Block, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the block list,
			return (long[])m.Arguments[1].Value;
		}

		public void ProcessSendBlock(long blockId, IServiceAddress sourceBlockServer, IServiceAddress destBlockServer, long destServerSguid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] man = ManagerServers;
			if (man == null || man.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress[] managerServers = new IServiceAddress[man.Length];
			for (int i = 0; i < man.Length; i++) {
				managerServers[i] = man[i].Address;
			}

			RequestMessage request = new RequestMessage("sendBlockTo");
			request.Arguments.Add(blockId);
			request.Arguments.Add(destBlockServer);
			request.Arguments.Add(destServerSguid);
			request.Arguments.Add(managerServers);

			Message m = Command(sourceBlockServer, ServiceType.Block, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}