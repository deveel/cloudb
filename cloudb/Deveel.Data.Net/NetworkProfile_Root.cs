using System;
using System.Collections.Generic;

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
			MessageStream outputStream = new MessageStream(12);
			outputStream.AddMessage("checkPathType", pathType);

			Message m = Command(root, ServiceType.Root, outputStream);
			if (m.IsError)
				throw new NetworkAdminException("Type '" + pathType + "' doesn't instantiate on the root");

			IServiceAddress managerServer = man.Address;

			// Create a new empty database,
			NetworkClient client = new NetworkClient(managerServer, network_connector);
			DataAddress dataAddress = client.CreateEmptyDatabase();
			client.Disconnect();

			// Perform the command,
			outputStream = new MessageStream(12);
			outputStream.AddMessage("addPath", pathName, pathType, dataAddress);
			outputStream.AddMessage("initPath", pathName);

			Message message = Command(root, ServiceType.Root, outputStream);
			if (message.IsError)
				throw new NetworkAdminException(message.ErrorMessage);

			// Tell the manager server about this path,
			outputStream = new MessageStream(7);
			outputStream.AddMessage("addPathRootMapping", pathName, root);

			message = Command(managerServer, ServiceType.Manager, outputStream);
			if (message.IsError)
				throw new NetworkAdminException(message.ErrorMessage);
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
			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage("removePath", path_name);

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

		public String GetPathStats(IServiceAddress root, string pathName) {
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

		public PathProfile[] GetPathsFromRoot(IServiceAddress root) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(root);

			MessageStream msg_out = new MessageStream(7);
			msg_out.AddMessage(new Message("pathReport"));

			Message m = Command(root, ServiceType.Root, msg_out);
			if (m.IsError)
				throw new NetworkAdminException(m.ErrorMessage);

			string[] paths = (string[])m[0];
			string[] funs = (string[])m[1];

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