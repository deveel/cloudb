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
			RequestMessage request = new RequestMessage(messageName);
			request.Arguments.Add(address);

			ResponseMessage m = Command(current_manager.Address, ServiceType.Manager, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage, m.ErrorStackTrace);
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

			string messageName = serviceType == ServiceType.Block ? "unregisterBlockServer" : "unregisterRootServer";
			RequestMessage request = new RequestMessage(messageName);
			request.Arguments.Add(address);

			ResponseMessage m = Command(current_manager.Address, ServiceType.Manager, request);
			if (m.HasError)
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

			RequestMessage request = new RequestMessage("getRootFor");
			request.Arguments.Add(pathName);

			ResponseMessage m = Command(manager_server, ServiceType.Manager, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

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

			RequestMessage request = new RequestMessage("getBlockMappingCount");
			ResponseMessage m = Command(manager_server, ServiceType.Manager, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long)m.Arguments[0].Value;
		}

		public long[] GetBlockMappingRange(long p1, long p2) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			RequestMessage request = new RequestMessage("getBlockMappingRange");
			request.Arguments.Add(p1);
			request.Arguments.Add(p2);

			ResponseMessage m = Command(manager_server, ServiceType.Manager, request);
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

			RequestMessage request = new RequestMessage("getRegisteredServerList");

			ResponseMessage m = Command(manager_server, ServiceType.Manager, request);
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

			RequestMessage request = new RequestMessage("addBlockServerMapping");
			request.Arguments.Add(block_id);
			request.Arguments.Add(server_guid);

			ResponseMessage m = Command(manager_server, ServiceType.Manager, request);
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

			RequestMessage request = new RequestMessage("removeBlockServerMapping");
			request.Arguments.Add(block_id);
			request.Arguments.Add(server_guid);

			ResponseMessage m = Command(manager_server, ServiceType.Manager, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}