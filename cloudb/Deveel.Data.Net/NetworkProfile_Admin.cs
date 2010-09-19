using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		private void InspectNetwork() {
			// If cached,
			if (machine_profiles == null) {
				// The sorted list of all servers in the schema,
				IServiceAddress[] slist = ServiceAddresses;

				List<MachineProfile> machines = new List<MachineProfile>();

				foreach (IServiceAddress server in slist) {
					MachineProfile machine_profile = new MachineProfile(server);

					// Request a report from the administration role on the machine,
					IMessageProcessor mp = network_connector.Connect(server, ServiceType.Admin);
					MessageStream msg_out = new MessageStream(16);
					msg_out.AddMessage(new Message("report"));
					MessageStream msg_in = mp.Process(msg_out);
					Message last_m = null;

					foreach (Message m in msg_in) {
						last_m = m;
					}

					if (last_m.IsError) {
						machine_profile.ErrorState = last_m.ErrorMessage;
					} else {
						// Get the message replies,
						string b = (string)last_m[0];
						bool is_block = !b.Equals("block=no");
						String m = (String)last_m[1];
						bool is_manager = !m.Equals("manager=no");
						string r = (string)last_m[2];
						bool is_root = !r.Equals("root=no");

						long used_mem = (long)last_m[3];
						long total_mem = (long)last_m[4];
						long used_disk = (long)last_m[5];
						long total_disk = (long)last_m[6];

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

		private void ChangeRole(MachineProfile machine, string status, String role_type) {
			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(status, role_type);
			Message m = Command(machine.Address, ServiceType.Admin, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Update the network profile,
			if (status.Equals("init")) {
				ServiceType type = (ServiceType)Enum.Parse(typeof(ServiceType), role_type, true);
				machine.ServiceType |= type;
			}
		}

		public bool IsValidNode(IServiceAddress machine) {
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

		public void StartService(IServiceAddress machine, ServiceType serviceType) {
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

			ChangeRole(machine_p, "init", serviceType.ToString().ToLower());
		}

		public void StopService(IServiceAddress machine, ServiceType serviceType) {
			if (serviceType == ServiceType.Admin)
				throw new ArgumentException("Invalid service type.", "serviceType");

			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(machine);
			if ((machine_p.ServiceType & serviceType) == 0)
				throw new NetworkAdminException("Manager not assigned to machine " + machine);

			ChangeRole(machine_p, "dispose", serviceType.ToString().ToLower());
		}

		public AnalyticsRecord[] GetAnalyticsStats(IServiceAddress server) {
			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("reportStats"));
			Message m = Command(server, ServiceType.Admin, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			long[] stats = (long[])m[0];
			int sz = stats.Length;

			List<AnalyticsRecord> records = new List<AnalyticsRecord>(sz / 4);

			for (int i = 0; i < sz; i += 4) {
				records.Add(new AnalyticsRecord(stats[i], stats[i + 1], stats[i + 2], stats[i + 3]));
			}

			return records.ToArray();
		}
	}
}