using System;
using System.Collections.Generic;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		private void SendManagerCommand(string functionName, params object[] args) {
			// Send the add path command to the first available manager server.
			MachineProfile[] managerServers = ManagerServers;

			RequestMessage message = new RequestMessage(functionName);
			foreach (object obj in args) {
				message.Arguments.Add(obj);
			}

			// The first manager that takes the command,
			Message lastError = null;
			for (int i = 0; i < managerServers.Length && lastError == null; ++i) {
				IServiceAddress managerServer = managerServers[i].Address;
				Message m = Command(managerServer, ServiceType.Manager, message);
				if (m.HasError) {
					if (!IsConnectionFailure(m))
						throw new NetworkAdminException(m.ErrorMessage);

					lastError = m;
				}
			}
			// All managers failed,
			if (lastError != null)
				throw new NetworkAdminException(lastError.ErrorMessage);
		}

		private object SendManagerFunction(String functionName, params object[] args) {
			// Send the add path command to the first available manager server.
			MachineProfile[] managerServers = ManagerServers;

			RequestMessage request = new RequestMessage(functionName);
			foreach (object obj in args) {
				request.Arguments.Add(obj);
			}

			// The first manager that takes the command,
			Message lastError = null;
			for (int i = 0; i < managerServers.Length; ++i) {
				IServiceAddress manager_server = managerServers[i].Address;
				Message m = Command(manager_server, ServiceType.Manager, request);
				if (m.HasError) {
					if (!IsConnectionFailure(m)) {
						throw new NetworkAdminException(m.ErrorMessage);
					}
					lastError = m;
				} else {
					return m.Arguments[0].Value;
				}
			}

			// All managers failed,
			throw new NetworkAdminException(lastError.ErrorMessage);
		}

		private void RegisterService(IServiceAddress address, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(address);
			MachineProfile[] currentManagers = ManagerServers;

			if (currentManagers == null || currentManagers.Length == 0)
				throw new NetworkAdminException("No manager server found");

			if ((machine_p.ServiceType & serviceType) == 0)
				throw new NetworkAdminException("Machine '" + address + "' is not assigned as a " + serviceType.ToString().ToLower() +
												" role");

			string messageName;
			if (serviceType == ServiceType.Manager)
				messageName = "registerManagerServer";
			else if (serviceType == ServiceType.Root)
				messageName = "registerRootServer";
			else 
				messageName = "registerBlockServer";

			RequestMessage request = new RequestMessage(messageName);
			request.Arguments.Add(address);

			for (int i = 0; i < currentManagers.Length; i++) {
				Message m = Command(currentManagers[i].Address, ServiceType.Manager, request);
				if (m.HasError)
					throw new NetworkAdminException(m.ErrorMessage, m.ErrorStackTrace);
			}
		}

		private void DeregisterService(IServiceAddress address, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(address);
			MachineProfile[] currentManagers = ManagerServers;

			if (currentManagers == null || currentManagers.Length == 0)
				throw new NetworkAdminException("No manager server found");

			if ((machine_p.ServiceType & serviceType) == 0)
				throw new NetworkAdminException("Machine '" + address + "' is not assigned as a " + serviceType.ToString().ToLower() +
												" role");

			string messageName;
			if (serviceType == ServiceType.Manager)
				messageName = "unregisterManagerServer";
			else if (serviceType == ServiceType.Root)
				messageName = "unregisterRootServer";
			else
				messageName = "unregisterBlockServer";

			RequestMessage request = new RequestMessage(messageName);
			request.Arguments.Add(address);

			for (int i = 0; i < currentManagers.Length; i++) {
				Message m = Command(currentManagers[i].Address, ServiceType.Manager, request);
				if (m.HasError)
					throw new NetworkAdminException(m.ErrorMessage);
			}
		}

		public void RegisterManager(IServiceAddress manager) {
			RegisterService(manager, ServiceType.Manager);
		}

		public void DeregisterManager(IServiceAddress manager) {
			DeregisterService(manager, ServiceType.Manager);
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

		public long GetBlockMappingCount() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] man = ManagerServers;
			if (man == null || man.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man[0].Address;

			RequestMessage request = new RequestMessage("getBlockMappingCount");
			Message m = Command(manager_server, ServiceType.Manager, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long)m.Arguments[0].Value;
		}

		public long[] GetBlockMappingRange(long p1, long p2) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] man = ManagerServers;
			if (man == null || man.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man[0].Address;

			RequestMessage request = new RequestMessage("getBlockMappingRange");
			request.Arguments.Add(p1);
			request.Arguments.Add(p2);

			Message m = Command(manager_server, ServiceType.Manager, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long[])m.Arguments[0].Value;
		}

		public IDictionary<IServiceAddress, String> GetBlocksStatus() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] man = ManagerServers;
			if (man == null || man.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man[0].Address;

			RequestMessage request = new RequestMessage("getRegisteredServerList");

			Message m = Command(manager_server, ServiceType.Manager, request);
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

		public void AddBlockAssociation(long blockId, long serverGuid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] man = ManagerServers;
			if (man == null || man.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress[] managerServers = new IServiceAddress[man.Length];
			for (int i = 0; i < man.Length; i++) {
				managerServers[i] = man[i].Address;
			}

			RequestMessage request = new RequestMessage("internalAddBlockServerMapping");
			request.Arguments.Add(blockId);
			request.Arguments.Add(new long[] {serverGuid});

			Message lastError = null;
			for (int i = 0; i < managerServers.Length; ++i) {
				Message response = Command(managerServers[i], ServiceType.Manager, request);
				if (response.HasError) 
					lastError = response;
			}

			if (lastError != null)
				throw new NetworkAdminException(lastError.ErrorMessage);
		}

		public void RemoveBlockAssociation(long blockId, long serverGuid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] man = ManagerServers;
			if (man == null || man.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress[] managerAddresses = new IServiceAddress[man.Length];
			for (int i = 0; i < man.Length; i++) {
				managerAddresses[i] = man[i].Address;
			}

			RequestMessage request = new RequestMessage("internalRemoveBlockServerMapping");
			request.Arguments.Add(blockId);
			request.Arguments.Add(new long[] {serverGuid});

			Message lastError = null;
			for (int i = 0; i < managerAddresses.Length; i++) {
				Message m = Command(managerAddresses[i], ServiceType.Manager, request);
				if (m.HasError)
					lastError = m;
			}

			
			if (lastError != null)
				throw new NetworkAdminException(lastError.ErrorMessage);
		}
	}
}