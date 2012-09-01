using System;
using System.Collections.Generic;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public partial class NetworkProfile {
		private NetworkConfigSource networkConfig;
		private readonly IServiceConnector networkConnector;

		private List<MachineProfile> machineProfiles;

		public NetworkProfile(IServiceConnector connector) {
			networkConnector = connector;
		}

		internal static bool IsConnectionFailure(Message m) {
			MessageError et = m.Error;
			// If it's a connect exception,
			string exType = et.ErrorType;
			if (exType.Equals("System.Net.Sockets.SocketException"))
				return true;
			if (exType.Equals("Deveel.Data.Net.ServiceNotConnectedException"))
				return true;
			return false;
		}

		public IEnumerable<IServiceAddress> SortedServers {
			get {
				IServiceAddress[] nodeList = networkConfig.NetworkNodes;
				if (nodeList == null || nodeList.Length == 0)
					return new IServiceAddress[0];

				List<IServiceAddress> slist = new List<IServiceAddress>(nodeList);
				// Sort the list of service addresses (the list is probably already sorted)
				slist.Sort();
				return slist.AsReadOnly();
			}
		}

		public NetworkConfigSource Configuration {
			get { return networkConfig; }
			set { networkConfig = value; }
		}

		private List<MachineProfile> InspectNetwork() {
			// If cached,
			if (machineProfiles != null) {
				return machineProfiles;
			}

			// The sorted list of all servers in the schema,
			IEnumerable<IServiceAddress> slist = SortedServers;

			// The list of machine profiles,
			List<MachineProfile> machines = new List<MachineProfile>();

			// For each machine in the network,
			foreach (IServiceAddress server in slist) {

				MachineProfile machineProfile = new MachineProfile(server);

				// Request a report from the administration role on the machine,
				IMessageProcessor mp = networkConnector.Connect(server, ServiceType.Admin);
				Message message = new Message("report");
				IEnumerable<Message> response = mp.Process(message.AsStream());
				Message lastM = null;

				foreach (Message m in response) {
					lastM = m;
				}

				if (lastM.HasError) {
					machineProfile.ErrorMessage = lastM.ErrorMessage;
				} else {
					// Get the message replies,
					String b = (String) lastM.Arguments[0].Value;
					bool isBlock = !b.Equals("block_server=no");
					String m = (String) lastM.Arguments[1].Value;
					bool isManager = !m.Equals("manager_server=no");
					String r = (String) lastM.Arguments[2].Value;
					bool isRoot = !r.Equals("root_server=no");

					long usedMem = (long) lastM.Arguments[3].Value;
					long totalMem = (long) lastM.Arguments[4].Value;
					long usedDisk = (long) lastM.Arguments[5].Value;
					long totalDisk = (long) lastM.Arguments[6].Value;

					// Populate the lists,
					machineProfile.IsBlock = isBlock;
					machineProfile.IsRoot = isRoot;
					machineProfile.IsManager = isManager;

					machineProfile.MemoryUsed = usedMem;
					machineProfile.MemoryTotal = totalMem;
					machineProfile.DiskUsed = usedDisk;
					machineProfile.DiskTotal = totalDisk;

				}

				// Add the machine profile to the list
				machines.Add(machineProfile);

			}

			machineProfiles = machines;
			return machineProfiles;
		}

		private Message Command(IServiceAddress machine, ServiceType serviceType, MessageStream message) {
			IMessageProcessor proc = networkConnector.Connect(machine, serviceType);
			IEnumerable<Message> msgIn = proc.Process(message);
			Message lastM = null;
			foreach (Message m in msgIn) {
				lastM = m;
			}
			return lastM;
		}

		private void SendManagerCommand(string functionName, params object[] args) {
			// Send the add path command to the first available manager server.
			MachineProfile[] managerServers = GetManagerServers();

			Message message = new Message(functionName, args);

			// The first manager that takes the command,
			bool success = false;
			Message lastError = null;
			for (int i = 0; i < managerServers.Length && success == false; ++i) {
				IServiceAddress managerServer = managerServers[i].ServiceAddress;
				Message m = Command(managerServer, ServiceType.Manager, message.AsStream());
				if (m.HasError) {
					if (!IsConnectionFailure(m)) {
						throw new NetworkAdminException(m.ErrorMessage);
					}
					lastError = m;
				} else {
					success = true;
				}
			}

			// All managers failed,
			if (!success) {
				throw new NetworkAdminException(lastError.ErrorMessage);
			}
		}

		private object SendManagerFunction(string functionName, params object[] args) {
			// Send the add path command to the first available manager server.
			MachineProfile[] managerServers = GetManagerServers();

			Message message = new Message(functionName, args);

			// The first manager that takes the command,
			Object result = null;
			Message lastError = null;
			for (int i = 0; i < managerServers.Length && result == null; ++i) {
				IServiceAddress managerServer = managerServers[i].ServiceAddress;
				Message m = Command(managerServer, ServiceType.Manager, message.AsStream());
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

		private void SendAllRootServers(IServiceAddress[] roots, string functionName, params object[] args) {
			Message message = new Message(functionName, args);

			// Send the command to all the root servers,
			Message lastError = null;
			IEnumerable<Message>[] responses = new IEnumerable<Message>[roots.Length];

			for (int i = 0; i < roots.Length; ++i) {
				IServiceAddress rootServer = roots[i];
				IMessageProcessor proc = networkConnector.Connect(rootServer, ServiceType.Root);
				responses[i] = proc.Process(message.AsStream());
			}

			int successCount = 0;
			foreach (MessageStream response in responses) {
				foreach (Message m in response) {
					if (m.HasError) {
						if (!IsConnectionFailure(m))
							throw new NetworkAdminException(m.ErrorMessage);

						lastError = m;
					} else {
						++successCount;
					}
				}
			}

			// Any one root failed,
			if (successCount != roots.Length) {
				throw new NetworkAdminException(lastError.ErrorMessage);
			}
		}

		private void SendRootServer(IServiceAddress root, String functionName, params object[] args) {
			Message message = new Message(functionName, args);

			// Send the command to all the root servers,
			Message lastError = null;

			IMessageProcessor proc = networkConnector.Connect(root, ServiceType.Root);
			IEnumerable<Message> response = proc.Process(message.AsStream());

			int successCount = 0;
			foreach (Message m in response) {
				if (m.HasError) {
					if (!IsConnectionFailure(m)) {
						throw new NetworkAdminException(m.ErrorMessage);
					}
					lastError = m;
				} else {
					++successCount;
				}
			}

			// Any one root failed,
			if (successCount != 1) {
				throw new NetworkAdminException(lastError.ErrorMessage);
			}
		}

		private MachineProfile CheckMachineInNetwork(IServiceAddress machine) {
			InspectNetwork();

			foreach (MachineProfile m in machineProfiles) {
				if (m.ServiceAddress.Equals(machine)) {
					return m;
				}
			}

			throw new NetworkAdminException("Machine '" + machine + "' is not in the network schema");
		}

		private void ChangeRole(MachineProfile machine, string status, ServiceType roleType) {
			Message message = new Message(status);
			message.Arguments.Add((byte)roleType);

			Message m = Command(machine.ServiceAddress, ServiceType.Admin, message.AsStream());
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}
			// Success,

			// Update the network profile,
			if (roleType == ServiceType.Manager) {
				machine.IsManager = status.Equals("start");
			} else if (roleType == ServiceType.Root) {
				machine.IsRoot = status.Equals("start");
			} else if (roleType == ServiceType.Block) {
				machine.IsBlock = status.Equals("start");
			}
		}


		public void Refresh() {
			machineProfiles = null;
			InspectNetwork();
		}

		public MachineProfile GetMachineProfile(IServiceAddress address) {
			InspectNetwork();
			foreach (MachineProfile p in machineProfiles) {
				if (p.ServiceAddress.Equals(address)) {
					return p;
				}
			}
			return null;
		}

		public MachineProfile[] GetManagerServers() {
			InspectNetwork();

			List<MachineProfile> list = new List<MachineProfile>();
			foreach (MachineProfile machine in machineProfiles) {
				if (machine.IsManager) {
					list.Add(machine);
				}
			}
			return list.ToArray();
		}

		public MachineProfile[] GetRootServers() {
			InspectNetwork();

			List<MachineProfile> list = new List<MachineProfile>();
			foreach (MachineProfile machine in machineProfiles) {
				if (machine.IsRoot) {
					list.Add(machine);
				}
			}

			return list.ToArray();
		}

		public MachineProfile[] GetBlockServers() {
			InspectNetwork();

			List<MachineProfile> list = new List<MachineProfile>();
			foreach (MachineProfile machine in machineProfiles) {
				if (machine.IsBlock) {
					list.Add(machine);
				}
			}

			return list.ToArray();
		}

		public MachineProfile[] GetAllMachineProfiles() {
			InspectNetwork();

			return machineProfiles.ToArray();
		}

		public void StartService(IServiceAddress machine, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(machine);
			if (!machineP.IsInRole(serviceType)) {
				// No current manager, so go ahead and assign,
				ChangeRole(machineP, "start", serviceType);
			} else {
				throw new NetworkAdminException("Manager already assigned on machine " + machine);
			}
		}
		
		public void StopService(IServiceAddress machine, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(machine);
			if (machineP.IsInRole(serviceType)) {
				// The current manager matches, so we can stop
				ChangeRole(machineP, "stop", serviceType);
			} else {
				throw new NetworkAdminException("Manager not assigned to machine " + machine);
			}
		}

		public void RegisterManager(IServiceAddress manager) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(manager);
			MachineProfile[] currentManagers = GetManagerServers();

			if (currentManagers.Length == 0)
				throw new NetworkAdminException("No manager server found");

			// Check it is a manager server,
			if (!machineP.IsManager)
				throw new NetworkAdminException("Machine '" + manager + "' is not assigned as a manager");

			// The list of manager servers,
			IServiceAddress[] managerServers = new IServiceAddress[currentManagers.Length];
			for (int i = 0; i < currentManagers.Length; ++i) {
				managerServers[i] = currentManagers[i].ServiceAddress;
			}

			Message message = new Message("registerManagerServers", new object[] {managerServers});

			// Register the root server with all the managers currently on the network,
			for (int i = 0; i < currentManagers.Length; ++i) {
				Message m = Command(currentManagers[i].ServiceAddress, ServiceType.Manager, message.AsStream());
				if (m.HasError) {
					throw new NetworkAdminException(m.ErrorMessage);
				}
			}
		}

		public void DeregisterManager(IServiceAddress root) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(root);
			MachineProfile[] currentManagers = GetManagerServers();

			if (currentManagers.Length == 0)
				throw new NetworkAdminException("No manager server found");

			// Check it is a manager server,
			if (!machineP.IsManager)
				throw new NetworkAdminException("Machine '" + root + "' is not assigned as a manager");

			Message message = new Message("deregisterManagerServer", root);

			// Register the root server with all the managers currently on the network,
			for (int i = 0; i < currentManagers.Length; ++i) {
				Message m = Command(currentManagers[i].ServiceAddress, ServiceType.Manager, message.AsStream());
				if (m.HasError)
					throw new NetworkAdminException(m.ErrorMessage);
			}
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

		public void RegisterService(IServiceAddress address, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(address);
			MachineProfile[] currentManagers = GetManagerServers();

			if (currentManagers.Length == 0)
				throw new NetworkAdminException("No manager server found");

			// Check it is a root server,
			if (!machineP.IsInRole(serviceType))
				throw new NetworkAdminException("Machine '" + address + "' is assigned as a " + serviceType);

			string command = null;
			if (serviceType == ServiceType.Manager) {
				RegisterManager(address);
			} else if (serviceType == ServiceType.Root) {
				command = "registerRootServer";
			} else if (serviceType == ServiceType.Block) {
				command = "registerBlockServer";
			} else {
				throw new ArgumentException();
			}

			Message message = new Message(command, address);

			// Register the root server with all the managers currently on the network,
			for (int i = 0; i < currentManagers.Length; ++i) {
				Message m = Command(currentManagers[i].ServiceAddress, ServiceType.Manager, message.AsStream());
				if (m.HasError)
					throw new NetworkAdminException(m.ErrorMessage);
			}
		}

		public void DeregisterService(IServiceAddress address, ServiceType serviceType) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(address);
			MachineProfile[] currentManagers = GetManagerServers();

			if (currentManagers.Length == 0)
				throw new NetworkAdminException("No manager server found");

			// Check it is a root server,
			if (!machineP.IsInRole(serviceType))
				throw new NetworkAdminException("Machine '" + address + "' is not assigned as a " + serviceType);

			string command = null;
			if (serviceType == ServiceType.Manager) {
				DeregisterManager(address);
			} else if (serviceType == ServiceType.Root) {
				command = "deregisterRootServer";
			} else if (serviceType == ServiceType.Block) {
				command = "deregisterBlockServer";
			} else {
				throw new ArgumentException();
			}

			Message message = new Message(command, address);

			for (int i = 0; i < currentManagers.Length; ++i) {
				Message m = Command(currentManagers[i].ServiceAddress, ServiceType.Manager, message.AsStream());
				if (m.HasError) {
					throw new NetworkAdminException(m.ErrorMessage);
				}
			}
		}

		public String[] GetAllPathNames() {
			InspectNetwork();

			// The list of all paths,
			return (string[]) SendManagerFunction("getAllPaths");
		}

		public PathInfo GetPathInfo(string pathName) {
			// Query the manager cluster for the PathInfo
			return (PathInfo) SendManagerFunction("getPathInfoForPath", pathName);
		}

		public void AddPathToNetwork(string pathName, string pathType, IServiceAddress rootLeader,
		                             IServiceAddress[] rootServers) {
			InspectNetwork();

			// Send the add path command to the first available manager server.
			SendManagerCommand("addPathToNetwork", pathName, pathType, rootLeader, rootServers);

			// Fetch the path info from the manager cluster,
			PathInfo pathInfo = (PathInfo) SendManagerFunction("getPathInfoForPath", pathName);

			// Send command to all the root servers,
			SendAllRootServers(rootServers, "internalSetPathInfo", pathName, pathInfo.VersionNumber, pathInfo);
			SendAllRootServers(rootServers, "loadPathInfo", pathInfo);

			// Initialize the path on the leader,
			SendRootServer(rootLeader, "initialize", pathInfo.PathName, pathInfo.VersionNumber);
		}

		public void RemovePathFromNetwork(String pathName, IServiceAddress rootServer) {
			InspectNetwork();

			// Send the remove path command to the first available manager server.
			SendManagerCommand("removePathFromNetwork", pathName, rootServer);
		}

		public DataAddress[] GetHistoricalPathRoots(IServiceAddress root, String pathName, long timestamp, int maxCount) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(root);
			// Check it's root,
			if (!machineP.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Perform the command,
			Message message = new Message("getPathHistorical", pathName, timestamp, timestamp);

			Message m = Command(root, ServiceType.Root, message.AsStream());
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}

			// Return the data address array,
			return (DataAddress[]) m.Arguments[0].Value;
		}

		public void SetPathRoot(IServiceAddress root, String pathName, DataAddress address) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(root);
			// Check it's root,
			if (!machineP.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Perform the command,
			Message message = new Message("publishPath", pathName, address);

			Message m = Command(root, ServiceType.Root, message.AsStream());
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}
		}

		public String GetPathStats(PathInfo pathInfo) {
			InspectNetwork();

			IServiceAddress rootLeader = pathInfo.RootLeader;

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(rootLeader);
			// Check it's root,
			if (!machineP.IsRoot)
				throw new NetworkAdminException("Machine '" + rootLeader + "' is not a root");

			// Perform the command,
			Message message = new Message("getPathStats", pathInfo.PathName, pathInfo.VersionNumber);

			Message m = Command(rootLeader, ServiceType.Root, message.AsStream());
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the stats string for this path
			return (String) m.Arguments[0].Value;
		}

		public long GetBlockGuid(IServiceAddress block) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(block);
			// Check it's a block server,
			if (!machineP.IsBlock)
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");

			Message message = new Message("serverGUID", block);

			Message m = Command(block, ServiceType.Block, message.AsStream());
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the GUID
			return (long) m.Arguments[0].Value;
		}

		public long GetBlockMappingCount() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] mans = GetManagerServers();
			if (mans.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress managerServer = mans[0].ServiceAddress;

			Message message = new Message("getBlockMappingCount");

			Message m = Command(managerServer, ServiceType.Manager, message.AsStream());
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long) m.Arguments[0].Value;
		}

		public long[] GetBlockMappingRange(long p1, long p2) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] mans = GetManagerServers();
			if (mans.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress managerServer = mans[0].ServiceAddress;

			Message message = new Message("getBlockMappingRange", p1, p2);
			Message m = Command(managerServer, ServiceType.Manager, message.AsStream());
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the service address for the root server,
			return (long[]) m.Arguments[0].Value;
		}

		public IDictionary<IServiceAddress, ServiceStatus> GetBlocksStatus() {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] mans = GetManagerServers();
			if (mans.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress managerServer = mans[0].ServiceAddress;

			Message message = new Message("getRegisteredServerList");
			Message m = Command(managerServer, ServiceType.Manager, message.AsStream());
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// The list of block servers registered with the manager,
			IServiceAddress[] regservers = (IServiceAddress[]) m.Arguments[0].Value;
			string[] regserversStatus = (string[]) m.Arguments[1].Value;

			Dictionary<IServiceAddress, ServiceStatus> map = new Dictionary<IServiceAddress, ServiceStatus>();
			for (int i = 0; i < regservers.Length; ++i) {
				map.Add(regservers[i], (ServiceStatus) Enum.Parse(typeof (ServiceStatus), regserversStatus[i], true));
			}

			// Return the map,
			return map;
		}

		public void AddBlockAssociation(long blockId, long serverGuid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] mans = GetManagerServers();
			if (mans.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress[] managerServers = new IServiceAddress[mans.Length];
			for (int i = 0; i < mans.Length; ++i) {
				managerServers[i] = mans[i].ServiceAddress;
			}

			// NOTE: This command will be propogated through all the other managers on
			//   the network by the manager.
			Message message = new Message("internalAddBlockServerMapping", blockId, new long[] {serverGuid});

			// Send the command to all the managers, if all fail throw an exception.
			bool success = false;
			Message lastError = null;
			for (int i = 0; i < managerServers.Length; ++i) {
				Message m = Command(managerServers[i], ServiceType.Manager, message.AsStream());
				if (m.HasError)
					lastError = m;
				else
					success = true;
			}
			if (!success) {
				throw new NetworkAdminException(lastError.ErrorMessage);
			}
		}

		public void RemoveBlockAssociation(long blockId, long serverGuid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] mans = GetManagerServers();
			if (mans.Length == 0)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress[] managerServers = new IServiceAddress[mans.Length];
			for (int i = 0; i < mans.Length; ++i) {
				managerServers[i] = mans[i].ServiceAddress;
			}

			// NOTE: This command will be propogated through all the other managers on
			//   the network by the manager.
			Message message = new Message("internalRemoveBlockServerMapping", blockId, new long[] {serverGuid});

			// Send the command to all the managers, if all fail throw an exception.
			bool success = false;
			Message lastError = null;
			for (int i = 0; i < managerServers.Length; ++i) {
				Message m = Command(managerServers[i], ServiceType.Manager, message.AsStream());
				if (m.HasError)
					lastError = m;
				else
					success = true;
			}

			if (!success) {
				throw new NetworkAdminException(lastError.ErrorMessage);
			}
		}

		public long[] GetBlockList(IServiceAddress block) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machineP = CheckMachineInNetwork(block);
			// Check it's a block server,
			if (!machineP.IsBlock) {
				throw new NetworkAdminException("Machine '" + block + "' is not a block role");
			}

			Message message = new Message("blockSetReport");
			Message m = Command(block, ServiceType.Block, message.AsStream());
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}

			// Return the block list,
			return (long[]) m.Arguments[1].Value;
		}

		public long[] GetAnalyticsStats(IServiceAddress server) {
			Message message = new Message("reportStats");
			Message m = Command(server, ServiceType.Admin, message.AsStream());
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}

			return (long[]) m.Arguments[0].Value;
		}

		public void ProcessSendBlock(long blockId, IServiceAddress sourceBlockServer, IServiceAddress destBlockServer, long destServerSguid) {
			InspectNetwork();

			// Get the current manager server,
			MachineProfile[] mans = GetManagerServers();
			if (mans.Length == 0) {
				throw new NetworkAdminException("No manager server found");
			}

			// Use the manager servers,
			IServiceAddress[] managerServers = new IServiceAddress[mans.Length];
			for (int i = 0; i < mans.Length; ++i) {
				managerServers[i] = mans[i].ServiceAddress;
			}

			Message message = new Message("sendBlockTo", blockId, destBlockServer, destServerSguid, managerServers);
			Message m = Command(sourceBlockServer, ServiceType.Block, message.AsStream());
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}
		}
	}
}