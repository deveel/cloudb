using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public sealed class NetworkProfile {
		public NetworkProfile(IServiceConnector connector, string network_password) {
			this.network_connector = connector;
			this.network_password = network_password;
		}

		private NetworkConfiguration network_config;
		private readonly IServiceConnector network_connector;
		private readonly String network_password;
		private List<MachineProfile> machine_profiles;

		public NetworkConfiguration Configuration {
			get { return network_config; }
			set {
				network_config = value;
				network_config.Reload();
			}
		}

		public List<ServiceAddress> SortedServerList {
			get {
				String node_list = network_config.NetworkNodelist;
				if (node_list == null || node_list.Length == 0)
					return new List<ServiceAddress>(0);

				List<ServiceAddress> slist = new List<ServiceAddress>();
				try {
					string[] nodes = node_list.Split(',');
					foreach (string node in nodes) {
						slist.Add(ServiceAddress.Parse(node.Trim()));
					}
				} catch (ApplicationException e) {
					throw new Exception("Unable to parse network configuration node list.", e);
				} catch (IOException e) {
					throw new Exception("IO Error parsing network configuration node list.", e);
				}

				// Sort the list of service addresses (the list is probably already sorted)
				slist.Sort();

				return slist;
			}
		}

		public MachineProfile ManagerServer {
			get {
				InspectNetwork();

				foreach (MachineProfile machine in machine_profiles) {
					if (machine.IsManager)
						return machine;
				}
				return null;
			}
		}

		public MachineProfile[] RootServers {
			get {
				InspectNetwork();

				List<MachineProfile> list = new List<MachineProfile>();
				foreach (MachineProfile machine in machine_profiles) {
					if (machine.IsRoot)
						list.Add(machine);
				}

				return list.ToArray();
			}
		}

		public MachineProfile[] BlockServers {
			get {
				InspectNetwork();

				List<MachineProfile> list = new List<MachineProfile>();
				foreach (MachineProfile machine in machine_profiles) {
					if (machine.IsBlock)
						list.Add(machine);
				}

				return list.ToArray();
			}
		}

		public MachineProfile[] MachineProfiles {
			get {
				InspectNetwork();
				return machine_profiles.ToArray();
			}
		}

		private void InspectNetwork() {
			// If cached,
			if (machine_profiles == null) {
				// The sorted list of all servers in the schema,
				List<ServiceAddress> slist = SortedServerList;

				List<MachineProfile> machines = new List<MachineProfile>();

				foreach(ServiceAddress server in slist) {
					MachineProfile machine_profile = new MachineProfile(server);

					// Request a report from the administration role on the machine,
					IMessageProcessor mp = network_connector.Connect(server, ServiceType.Admin);
					MessageStream msg_out = new MessageStream(16);
					msg_out.AddMessage(new Message("report"));
					MessageStream msg_in = mp.Process(msg_out);
					Message last_m = null;

					foreach(Message m in msg_in) {
						last_m = m;
					}

					if (last_m.IsError) {
						machine_profile.ErrorState = last_m.ErrorMessage;
					} else {
						// Get the message replies,
						string b = (string) last_m[0];
						bool is_block = !b.Equals("block_server=no");
						String m = (String) last_m[1];
						bool is_manager = !m.Equals("manager_server=no");
						string r = (string) last_m[2];
						bool is_root = !r.Equals("root_server=no");

						long used_mem = (long) last_m[3];
						long total_mem = (long) last_m[4];
						long used_disk = (long) last_m[5];
						long total_disk = (long) last_m[6];

						ServiceType type = new ServiceType();
						if (is_block)
							type |= ServiceType.Block;
						if (is_manager)
							type |= ServiceType.Manager;
						if (is_root)
							type |= ServiceType.Root;

						// Populate the lists,
						machine_profile.ServiceType = type;

						machine_profile.MemoryUsed = used_mem;
						machine_profile.MemoryTotal = total_mem;
						machine_profile.StorageUsed = used_disk;
						machine_profile.StorageTotal = total_disk;
					}

					// Add the machine profile to the list
					machines.Add(machine_profile);
				}

				machine_profiles = machines;
			}
		}

		private Message Command(ServiceAddress machine, ServiceType serviceType, MessageStream outputStream) {
			IMessageProcessor proc = network_connector.Connect(machine, serviceType);
			MessageStream inputStream = proc.Process(outputStream);
			Message message = null;
			foreach (Message m in inputStream) {
				message = m;
			}
			return message;
		}

		private MachineProfile CheckMachineInNetwork(ServiceAddress machine) {
			InspectNetwork();

			foreach (MachineProfile m in machine_profiles) {
				if (m.Address.Equals(machine))
					return m;
			}

			throw new NetworkAdminException("Machine '" + machine + "' is not in the network schema");
		}

		private void ChangeRole(MachineProfile machine, string status, String role_type) {
			MessageStream msg_out = new MessageStream(7);
			Message message = new Message(status);
			if (role_type.Equals("manager")) {
				message.AddArgument("manager_server");
			} else if (role_type.Equals("root")) {
				message.AddArgument("root_server");
			} else if (role_type.Equals("block")) {
				message.AddArgument("block_server");
			} else {
				throw new Exception("Unknown role type: " + role_type);
			}
			msg_out.AddMessage(message);

			Message m = Command(machine.Address, ServiceType.Admin, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Update the network profile,
			if (status.Equals("start")) {
				ServiceType type = (ServiceType) Enum.Parse(typeof(ServiceType), role_type, true);
				machine.ServiceType |= type;
			}
		}

		private void RegisterService(ServiceAddress address, ServiceType serviceType) {
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

		private void DeregisterService(ServiceAddress address, ServiceType serviceType) {
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


		public bool IsValidNode(ServiceAddress machine) {
			// Request a report from the administration role on the machine,
			IMessageProcessor mp = network_connector.Connect(machine, ServiceType.Admin);
			MessageStream msg_out = new MessageStream(16);
			msg_out.AddMessage(new Message("report"));
			MessageStream msg_in = mp.Process(msg_out);
			Message last_m = null;

			foreach (Message m in msg_in) {
				last_m = m;
			}

			if (last_m.IsError)
				// Not a valid node,
				// Should we break this error down to smaller questions. Such as, is the
				// password incorrect, etc?
				return false;

			return true;
		}

		public void Refresh() {
			machine_profiles = null;
			InspectNetwork();
		}

		public bool IsMachineInNetwork(ServiceAddress machine_addr) {
			InspectNetwork();

			foreach (MachineProfile machine in machine_profiles) {
				if (machine.Address.Equals(machine_addr)) {
					return true;
				}
			}
			return false;
		}

		public MachineProfile GetMachineProfile(ServiceAddress address) {
			InspectNetwork();

			foreach (MachineProfile p in machine_profiles) {
				if (p.Address.Equals(address))
					return p;
			}
			return null;
		}

		public void StartService(ServiceAddress machine, ServiceType serviceType) {
			if (serviceType == ServiceType.Admin)
				throw new ArgumentException("Invalid service type.", "serviceType");

			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(machine);
			if (serviceType == ServiceType.Manager) {
				MachineProfile current_manager = ManagerServer;
				if (current_manager != null)
					throw new NetworkAdminException("Manager already assigned on machine " + current_manager);
			}

			if ((machine_p.ServiceType & serviceType) != 0)
				throw new NetworkAdminException("Role '" + serviceType + "' already assigned on machine " + machine);

			ChangeRole(machine_p, "start", serviceType.ToString().ToLower());
		}

		public void StopService(ServiceAddress machine, ServiceType serviceType) {
			if (serviceType == ServiceType.Admin)
				throw new ArgumentException("Invalid service type.", "serviceType");

			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(machine);
			if ((machine_p.ServiceType & serviceType) == 0)
				throw new NetworkAdminException("Manager not assigned to machine " + machine);

			ChangeRole(machine_p, "stop", serviceType.ToString().ToLower());
		}

		public void RegisterRoot(ServiceAddress root) {
			RegisterService(root, ServiceType.Root);
		}

		public void DeregisterRoot(ServiceAddress root) {
			DeregisterService(root, ServiceType.Root);
		}

		public void RegisterBlock(ServiceAddress block) {
			RegisterService(block, ServiceType.Block);
		}

		public void DeregisterBlock(ServiceAddress block) {
			DeregisterService(block, ServiceType.Block);
		}

		public PathProfile[] GetPathsFromRoot(ServiceAddress root) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(root);

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("coordinationProcessorReport"));

			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			string[] paths = (string[]) m[0];
			string[] funs = (string[]) m[1];

			PathProfile[] list = new PathProfile[paths.Length];
			for (int i = 0; i < paths.Length; ++i) {
				list[i] = new PathProfile(root, paths[i], funs[i]);
			}

			return list;
		}

		public PathProfile[] GetPaths() {
			InspectNetwork();

			List<PathProfile> fullList = new List<PathProfile>();
			MachineProfile[] allRoots = RootServers;

			foreach (MachineProfile root in allRoots) {
				PathProfile[] list = GetPathsFromRoot(root.Address);
				fullList.AddRange(list);
			}

			// Return the full list as an array
			return fullList.ToArray();
		}

		public void AddCoordinationFunction(ServiceAddress root, string pathName, string coordinationFunction) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineProfile = CheckMachineInNetwork(root);
			if (!machineProfile.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			// Check with the root server that the class instantiates,
			MessageStream outputStream = new MessageStream(12);
			outputStream.AddMessage(new Message("checkCoordinationType", new object[] {coordinationFunction}));

			Message m = Command(root, ServiceType.Root, outputStream);
			if (m.IsError)
				throw new NetworkAdminException("Type '" + coordinationFunction + "' doesn't instantiate on the root");

			ServiceAddress managerServer = man.Address;

			// Create a new empty database,
			NetworkClient dbClient = NetworkClient.ConnectTcp(managerServer, network_password);
			DataAddress dataAddress = dbClient.CreateEmptyDatabase();
			dbClient.Disconnect();

			// Perform the command,
			outputStream = new MessageStream(12);
			outputStream.AddMessage(new Message("addCoordinationProcessor",
			                                    new object[] {pathName, coordinationFunction, dataAddress}));
			outputStream.AddMessage(new Message("initialize", new object[] {pathName}));

			Message message = Command(root, ServiceType.Root, outputStream);
			if (message.IsError)
				throw new NetworkAdminException(message.ErrorMessage);

			// Tell the manager server about this path,
			outputStream = new MessageStream(7);
			outputStream.AddMessage(new Message("addPathRootMapping", new object[] {pathName, root}));

			message = Command(managerServer, ServiceType.Manager, outputStream);
			if (message.IsError)
				throw new NetworkAdminException(message.ErrorMessage);
		}

		public void RemoveCoordinationFunction(ServiceAddress root, string path_name) {
			InspectNetwork();

			MachineProfile machine_p = CheckMachineInNetwork(root);
			if (!machine_p.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			ServiceAddress manager_server = man.Address;

			// Perform the command,
			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("removeCoordinationProcessor", new object[] {path_name}));

			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Tell the manager server to remove this path association,
			msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("removePathRootMapping", new object[] { path_name }));

			m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public ServiceAddress GetRoot(string pathName) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			ServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getRootFor", new object[] { pathName }));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (ServiceAddress)m[0];
		}

		public String GetPathStats(ServiceAddress root, string pathName) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(root);
			// Check it's root,
			if (!machine_p.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Perform the command,
			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getPathStats", new object[] { pathName }));

			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the stats string for this path
			return (string)m[0];
		}

		public long GetBlockGuid(ServiceAddress block) {
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

		public long GetBlockMappingCount() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			ServiceAddress manager_server = man.Address;

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

			ServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getBlockMappingRange", new object[] { p1, p2 }));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long[])m[0];
		}

		public IDictionary<ServiceAddress, String> GetBlocksStatus() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			ServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("getRegisteredServerList"));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// The list of block servers registered with the manager,
			ServiceAddress[] regservers = (ServiceAddress[])m[0];
			String[] regservers_status = (String[])m[1];

			Dictionary<ServiceAddress, string> map = new Dictionary<ServiceAddress, string>();
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

			ServiceAddress manager_server = man.Address;

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

			ServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("removeBlockServerMapping", new object[] { block_id, server_guid }));

			Message m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public long[] GetBlockList(ServiceAddress block) {
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

		public AnalyticsRecord[] GetAnalyticsStats(ServiceAddress server) {
			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("reportStats"));
			Message m = Command(server, ServiceType.Admin, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			long[] stats = (long[]) m[0];
			int sz = stats.Length;

			List<AnalyticsRecord> records = new List<AnalyticsRecord>(sz / 4);

			for (int i = 0; i < sz; i += 4) {
				records.Add(new AnalyticsRecord(stats[i], stats[i + 1], stats[i + 2], stats[i + 3]));
			}

			return records.ToArray();
		}

		public void ProcessSendBlock(long block_id, ServiceAddress source_block_server, ServiceAddress dest_block_server, long dest_server_sguid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			ServiceAddress manager_server = man.Address;

			MessageStream msg_out = new MessageStream(6);
			msg_out.AddMessage(new Message("sendBlockTo",
			                               new object[] {block_id, dest_block_server, dest_server_sguid, manager_server}));

			Message m = Command(source_block_server, ServiceType.Block, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public static NetworkProfile ConnectTcp(String network_password) {
			IServiceConnector connector = new TcpServiceConnector(network_password);
			NetworkProfile network_profile = new NetworkProfile(connector, network_password);
			return network_profile;
		}
	}
}