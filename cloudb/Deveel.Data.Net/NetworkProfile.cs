using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		public NetworkProfile(IServiceConnector connector) {
			this.network_connector = connector;
		}

		private NetworkConfiguration network_config;
		private readonly IServiceConnector network_connector;
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
	}
}