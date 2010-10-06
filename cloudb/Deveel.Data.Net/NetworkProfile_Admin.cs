using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

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
					MessageRequest request = new MessageRequest("report");
					Message msg_in = mp.ProcessMessage(request);

					if (MessageUtil.HasError(msg_in)) {
						machine_profile.ErrorState = MessageUtil.GetErrorMessage(msg_in);
					} else {
						// Get the message replies,
						string b = msg_in.Arguments[0].ToString();
						bool is_block = !b.Equals("block=no");
						String m = msg_in.Arguments[1].ToString();
						bool is_manager = !m.Equals("manager=no");
						string r = msg_in.Arguments[2].ToString();
						bool is_root = !r.Equals("root=no");

						long used_mem = msg_in.Arguments[3].ToInt64();
						long total_mem = msg_in.Arguments[4].ToInt64();
						long used_disk = msg_in.Arguments[5].ToInt64();
						long total_disk = msg_in.Arguments[6].ToInt64();

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
			MessageRequest request = new MessageRequest(status);
			request.Arguments.Add(role_type);
			Message m = Command(machine.Address, ServiceType.Admin, request);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));

			// Update the network profile,
			if (status.Equals("init")) {
				ServiceType type = (ServiceType)Enum.Parse(typeof(ServiceType), role_type, true);
				machine.ServiceType |= type;
			}
		}

		public bool IsValidNode(IServiceAddress machine) {
			// Request a report from the administration role on the machine,
			IMessageProcessor mp = network_connector.Connect(machine, ServiceType.Admin);
			MessageRequest msg_out = new MessageRequest();
			msg_out.Arguments.Add("report");
			Message msg_in = mp.ProcessMessage(msg_out);

			if (MessageUtil.HasError(msg_in))
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
			MessageRequest msg_out = new MessageRequest();
			msg_out.Arguments.Add("reportStats");
			Message m = Command(server, ServiceType.Admin, msg_out);
			if (MessageUtil.HasError(m))
				throw new NetworkAdminException(MessageUtil.GetErrorMessage(m));

			long[] stats = (long[])m.Arguments[0].Value;
			int sz = stats.Length;

			List<AnalyticsRecord> records = new List<AnalyticsRecord>(sz / 4);

			for (int i = 0; i < sz; i += 4) {
				records.Add(new AnalyticsRecord(stats[i], stats[i + 1], stats[i + 2], stats[i + 3]));
			}

			return records.ToArray();
		}
	}
}