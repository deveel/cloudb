using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		public NetworkProfile(IServiceConnector connector) {
			this.network_connector = connector;
		}

		private NetworkConfigSource network_config;
		private readonly IServiceConnector network_connector;
		private List<MachineProfile> machine_profiles = null;

		public NetworkConfigSource Configuration {
			get { return network_config; }
			set { network_config = value; }
		}

		public IServiceAddress[] ServiceAddresses {
			get {
				if (network_config == null)
					return new IServiceAddress[0];
				IServiceAddress[] node_list = network_config.NetworkNodes;
				// Sort the list of service addresses (the list is probably already sorted)
				Array.Sort(node_list);
				return node_list;
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

		private ResponseMessage Command(IServiceAddress machine, ServiceType serviceType, RequestMessage request) {
			IMessageProcessor proc = network_connector.Connect(machine, serviceType);
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