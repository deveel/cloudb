using System;
using System.Collections.Generic;

using Deveel.Data.Net.Client;

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

			string messageName = serviceType == ServiceType.Block ? "registerBlockServer" : "registerRootServer";
			MessageRequest msg_out = new MessageRequest(messageName);
			msg_out.Arguments.Add(address);

			Message m = Command(current_manager.Address, ServiceType.Manager, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));
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

			string messageName = serviceType == ServiceType.Block ? "deregisterBlockServer" : "deregisterRootServer";
			MessageRequest msg_out = new MessageRequest(messageName);
			msg_out.Arguments.Add(address);

			Message m = Command(current_manager.Address, ServiceType.Manager, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));
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

			MessageRequest msg_out = new MessageRequest("getRootFor");
			msg_out.Arguments.Add(pathName);

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));

			// Return the service address for the root server,
			return (IServiceAddress)m.Arguments[0].Value;
		}

		public long GetBlockMappingCount() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageRequest msg_out = new MessageRequest("getBlockMappingCount");
			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));

			// Return the service address for the root server,
			return m.Arguments[0].ToInt64();
		}

		public long[] GetBlockMappingRange(long p1, long p2) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageRequest msg_out = new MessageRequest("getBlockMappingRange");
			msg_out.Arguments.Add(p1);
			msg_out.Arguments.Add(p2);

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long[])m.Arguments[0].Value;
		}

		public IDictionary<IServiceAddress, String> GetBlocksStatus() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageRequest msg_out = new MessageRequest("getRegisteredServerList");
			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// The list of block servers registered with the manager,
			IServiceAddress[] regservers = (IServiceAddress[])m.Arguments[0].Value;
			String[] regservers_status = (String[])m.Arguments[1].Value;

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

			MessageRequest msg_out = new MessageRequest("addBlockServerMapping");
			msg_out.Arguments.Add(block_id);
			msg_out.Arguments.Add(server_guid);

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public void RemoveBlockAssociation(long block_id, long server_guid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			MessageRequest msg_out = new MessageRequest("removeBlockServerMapping");
			msg_out.Arguments.Add(block_id);
			msg_out.Arguments.Add(server_guid);

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}