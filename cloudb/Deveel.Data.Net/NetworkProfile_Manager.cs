using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		private void RegisterService(IServiceAddress address, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(address);
			MachineProfile current_manager = ManagerServer;

			if (current_manager == null)
				throw new NetworkAdminException("No manager server found");

			if ((machine_p.ServiceType & serviceType) == 0)
				throw new NetworkAdminException("Machine '" + address + "' is not assigned as a " + serviceType.ToString().ToLower() +
												" role");

			MessageStream msg_out = new MessageStream(7);
			string messageName = serviceType == ServiceType.Block ? "registerBlockServer" : "registerRootServer";
			msg_out.AddMessage(new Message(messageName, new object[] { address }));

			Message m = Command(current_manager.Address, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		private void DeregisterService(IServiceAddress address, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(address);
			MachineProfile current_manager = ManagerServer;

			if (current_manager == null)
				throw new NetworkAdminException("No manager server found");

			if ((machine_p.ServiceType & serviceType) == 0)
				throw new NetworkAdminException("Machine '" + address + "' is not assigned as a " + serviceType.ToString().ToLower() +
												" role");

			MessageStream msg_out = new MessageStream(7);
			string messageName = serviceType == ServiceType.Block ? "deregisterBlockServer" : "deregisterRootServer";
			msg_out.AddMessage(new Message(messageName, new object[] { address }));

			Message m = Command(current_manager.Address, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public void RegisterRoot(IServiceAddress root) {
			RegisterService(root, ServiceType.Root);
		}

		public void DeregisterRoot(IServiceAddress root) {
			DeregisterService(root, ServiceType.Root);
		}

		public void RegisterBlock(IServiceAddress block) {
			RegisterService(block, ServiceType.Block);
		}

		public void DeregisterBlock(IServiceAddress block) {
			DeregisterService(block, ServiceType.Block);
		}

		public IServiceAddress GetRoot(string pathName) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage("getRootFor", pathName);

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (IServiceAddress)m[0];
		}

		public long GetBlockMappingCount() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getBlockMappingCount"));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long)m[0];
		}

		public long[] GetBlockMappingRange(long p1, long p2) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getBlockMappingRange", new object[] { p1, p2 }));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long[])m[0];
		}

		public IDictionary<IServiceAddress, String> GetBlocksStatus() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getRegisteredServerList"));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// The list of block servers registered with the manager,
			IServiceAddress[] regservers = (IServiceAddress[])m[0];
			String[] regservers_status = (String[])m[1];

			Dictionary<IServiceAddress, string> map = new Dictionary<IServiceAddress, string>();
			for (int i = 0; i < regservers.Length; ++i)
				map.Add(regservers[i], regservers_status[i]);

			// Return the map,
			return map;
		}

		public void AddBlockAssociation(long block_id, long server_guid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("addBlockServerMapping", new object[] { block_id, server_guid }));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public void RemoveBlockAssociation(long block_id, long server_guid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("removeBlockServerMapping", new object[] { block_id, server_guid }));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}