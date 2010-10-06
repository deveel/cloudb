using System;
using System.Collections.Generic;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		public void AddPath(IServiceAddress root, string pathName, string pathType) {
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
			MessageRequest outputStream = new MessageRequest("checkPathType");
			outputStream.Arguments.Add(pathType);

			Message m = Command(root, ServiceType.Root, outputStream);
			if (m.HasError)
				throw new NetworkAdminException("Type '" + pathType + "' doesn't instantiate on the root");

			IServiceAddress managerServer = man.Address;

			// Create a new empty database,
			NetworkClient client = new NetworkClient(managerServer, network_connector);
			client.Connect();
			DataAddress dataAddress = client.CreateEmptyDatabase();
			client.Disconnect();

			// Perform the command,
			outputStream = new MessageRequest("addPath");
			outputStream.Arguments.Add(pathName);
			outputStream.Arguments.Add(pathType);
			outputStream.Arguments.Add(dataAddress);
			m = Command(root, ServiceType.Root, outputStream);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			outputStream = new MessageRequest("initPath");
			outputStream.Arguments.Add(pathName);

			m = Command(root, ServiceType.Root, outputStream);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Tell the manager server about this path,
			outputStream = new MessageRequest("addPathRootMapping");
			outputStream.Arguments.Add(pathName);
			outputStream.Arguments.Add(root);

			m = Command(managerServer, ServiceType.Manager, outputStream);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public void RemovePath(IServiceAddress root, string path_name) {
			InspectNetwork();

			MachineProfile machine_p = CheckMachineInNetwork(root);
			if (!machine_p.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Get the current manager server,
			MachineProfile man = ManagerServer;
			if (man == null)
				throw new NetworkAdminException("No manager server found");

			IServiceAddress manager_server = man.Address;

			// Perform the command,
			MessageRequest msg_out = new MessageRequest("removePath");
			msg_out.Arguments.Add(path_name);

			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Tell the manager server to remove this path association,
			msg_out = new MessageRequest("removePathRootMapping");
			msg_out.Arguments.Add(path_name);

			m = Command(manager_server, ServiceType.Manager, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}

		public String GetPathStats(IServiceAddress root, string pathName) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(root);
			// Check it's root,
			if (!machine_p.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Perform the command,
			MessageRequest msg_out = new MessageRequest("getPathStats");
			msg_out.Arguments.Add(pathName);

			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the stats string for this path
			return m.Arguments[0].ToString();
		}

		public PathProfile[] GetPathsFromRoot(IServiceAddress root) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(root);

			MessageRequest msg_out = new MessageRequest("pathReport");
			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			string[] paths = (string[])m.Arguments[0].Value;
			string[] funs = (string[])m.Arguments[1].Value;

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
	}
}