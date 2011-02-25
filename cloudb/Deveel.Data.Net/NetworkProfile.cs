using System;
using System.Collections.Generic;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		public NetworkProfile(IServiceConnector connector) {
			this.connector = connector;
		}

		private NetworkConfigSource config;
		private readonly IServiceConnector connector;
		private List<MachineProfile> machine_profiles = null;

		public NetworkConfigSource Configuration {
			get { return config; }
			set { config = value; }
		}

		public IServiceAddress[] ServiceAddresses {
			get {
				if (config == null)
					return new IServiceAddress[0];
				IServiceAddress[] node_list = config.NetworkNodes;
				// Sort the list of service addresses (the list is probably already sorted)
				Array.Sort(node_list);
				return node_list;
			}
		}

		public MachineProfile[] ManagerServers {
			get {
				InspectNetwork();

				List<MachineProfile> managers = new List<MachineProfile>();
				foreach (MachineProfile machine in machine_profiles) {
					if (machine.IsManager)
						managers.Add(machine);
				}
				return managers.ToArray();
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

		public IServiceConnector Connector {
			get { return connector; }
		}

		private Message Command(IServiceAddress machine, ServiceType serviceType, Message request) {
			IMessageProcessor proc = connector.Connect(machine, serviceType);
			return proc.Process(request);
		}

		private MachineProfile CheckMachineInNetwork(IServiceAddress machine) {
			InspectNetwork();

			foreach (MachineProfile m in machine_profiles) {
				if (m.Address.Equals(machine))
					return m;
			}

			throw new NetworkAdminException("Machine '" + machine + "' is not in the network schema");
		}

		private static bool IsConnectionFailure(Message m) {
			MessageError et = m.Error;
			// If it's a connect exception,
			string exType = et.Source;
			if (exType.Equals("System.Net.Sockets.SocketException"))
				return true;
			if (exType.Equals("Deveel.Data.Net.ServiceNotConnectedException"))
				return true;
			return false;
		}

		public void Refresh() {
			machine_profiles = null;
			InspectNetwork();
		}

		public bool IsMachineInNetwork(IServiceAddress machine_addr) {
			InspectNetwork();

			foreach (MachineProfile machine in machine_profiles) {
				if (machine.Address.Equals(machine_addr)) {
					return true;
				}
			}
			return false;
		}

		public MachineProfile GetMachineProfile(IServiceAddress address) {
			InspectNetwork();

			foreach (MachineProfile p in machine_profiles) {
				if (p.Address.Equals(address))
					return p;
			}
			return null;
		}
	}
}