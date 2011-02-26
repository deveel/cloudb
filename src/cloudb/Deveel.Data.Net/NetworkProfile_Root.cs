﻿using System;
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
			Message outputStream = new RequestMessage("checkPathType");
			outputStream.Arguments.Add(pathType);

			Message m = Command(root, ServiceType.Root, outputStream);
			if (m.HasError)
				throw new NetworkAdminException("Type '" + pathType + "' doesn't instantiate on the root");

			IServiceAddress managerServer = man.Address;

			// Create a new empty database,
			NetworkClient client = new NetworkClient(managerServer, connector);
			client.Connect();
			DataAddress dataAddress = client.CreateEmptyDatabase();
			client.Disconnect();

			// Perform the command,
			outputStream = new MessageStream(MessageType.Request);
			RequestMessage request = new RequestMessage("addPath");
			request.Arguments.Add(pathName);
			request.Arguments.Add(pathType);
			request.Arguments.Add(dataAddress);
			((MessageStream)outputStream).AddMessage(request);

			request = new RequestMessage("initPath");
			request.Arguments.Add(pathName);
			((MessageStream)outputStream).AddMessage(request);

			Message message = Command(root, ServiceType.Root, outputStream);
			if (message.HasError)
				throw new NetworkAdminException(message.ErrorMessage);

			// Tell the manager server about this path,
			outputStream = new RequestMessage("addPathRootMapping");
			outputStream.Arguments.Add(pathName);
			outputStream.Arguments.Add(root);

			message = Command(managerServer, ServiceType.Manager, outputStream);
			if (message.HasError)
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
			RequestMessage request = new RequestMessage("removePath");
			request.Arguments.Add(path_name);

			Message m = Command(root, ServiceType.Root, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Tell the manager server to remove this path association,
			request = new RequestMessage("removePathRootMapping");
			request.Arguments.Add(path_name);

			m = Command(manager_server, ServiceType.Manager, request);
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
			RequestMessage request = new RequestMessage("getPathStats");
			request.Arguments.Add(pathName);

			Message m = Command(root, ServiceType.Root, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the stats string for this path
			return m.Arguments[0].ToString();
		}

		public PathProfile[] GetPathsFromRoot(IServiceAddress root) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(root);

			RequestMessage request = new RequestMessage("pathReport");
			Message m = Command(root, ServiceType.Root, request);
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

		public DataAddress[] GetHistoricalPathRoots(IServiceAddress root, string pathName, DateTime time, int maxCount) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine = CheckMachineInNetwork(root);
			// Check it's root,
			if (!machine.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Perform the command,
			RequestMessage request = new RequestMessage("getPathHistorical");
			request.Arguments.Add(pathName);
			request.Arguments.Add(time.ToBinary());
			request.Arguments.Add(time.ToBinary());

			ResponseMessage m = (ResponseMessage) Command(root, ServiceType.Root, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);

			// Return the data address array,
			return (DataAddress[])m.ReturnValue;
		}

		public void PublishPath(IServiceAddress root, string pathName, DataAddress address) {
			InspectNetwork();

			// Check machine is in the schema,
			MachineProfile machine = CheckMachineInNetwork(root);
			// Check it's root,
			if (!machine.IsRoot)
				throw new NetworkAdminException("Machine '" + root + "' is not a root");

			// Perform the command,
			RequestMessage request = new RequestMessage("publishPath");
			request.Arguments.Add(pathName);
			request.Arguments.Add(address);

			Message m = Command(root, ServiceType.Root, request);
			if (m.HasError)
				throw new NetworkAdminException(m.ErrorMessage);
		}
	}
}