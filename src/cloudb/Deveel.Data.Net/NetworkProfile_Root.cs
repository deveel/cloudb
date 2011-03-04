using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed partial class NetworkProfile {
		private void SendAllRootServers(IServiceAddress[] roots, string function_name, params object[] args) {
			RequestMessage request = new RequestMessage(function_name);
			foreach (object obj in args) {
				request.Arguments.Add(obj);
			}

			// Send the command to all the root servers,
			Message lastError = null;
			Message[] responses = new Message[roots.Length];

			for (int i = 0; i < roots.Length; ++i) {
				IServiceAddress rootServer = roots[i];
				IMessageProcessor proc = connector.Connect(rootServer, ServiceType.Root);
				responses[i] = proc.Process(request);
			}

			int successCount = 0;
			foreach (Message response in responses) {
				if (response.HasError) {
					if (!IsConnectionFailure(response)) {
						throw new NetworkAdminException(response.ErrorMessage);
					}
					lastError = response;
				} else {
					++successCount;
				}
			}

			// Any one root failed,
			if (successCount != roots.Length) {
				throw new NetworkAdminException(lastError.ErrorMessage);
			}
		}

		private void SendRootServer(IServiceAddress root, string function_name, params object[] args) {
			RequestMessage request = new RequestMessage(function_name);
			foreach (object obj in args) {
				request.Arguments.Add(obj);
			}

			// Send the command to all the root servers,
			Message error = null;

			IMessageProcessor proc = connector.Connect(root, ServiceType.Root);
			Message response = proc.Process(request);

			if (response.HasError) {
				if (!IsConnectionFailure(response))
					throw new NetworkAdminException(response.ErrorMessage);

				error = response;
			}

			// Any one root failed,
			if (error != null)
				throw new NetworkAdminException(error.ErrorMessage);
		}

		public void AddPath(string pathName, string pathType, IServiceAddress rootLeader, IServiceAddress[] rootServers) {
			InspectNetwork();

			// Send the add path command to the first available manager server.
			SendManagerCommand("addPathToNetwork", pathName, pathType, rootLeader, rootServers);

			// Fetch the path info from the manager cluster,
			PathInfo path_info = (PathInfo) SendManagerFunction("getPathInfoForPath", pathName);

			// Send command to all the root servers,
			SendAllRootServers(rootServers, "internalSetPathInfo", pathName, path_info.Version, path_info);
			SendAllRootServers(rootServers, "loadPathInfo", path_info);

			// Initialize the path on the leader,
			SendRootServer(rootLeader, "initialize", path_info.PathName, path_info.Version);
		}

		public void RemovePath(string pathName, IServiceAddress rootServer) {
			InspectNetwork();

			// Send the remove path command to the first available manager server.
			SendManagerCommand("removePathFromNetwork", pathName, rootServer);
		}

		public string GetPathStats(PathInfo pathInfo) {
			InspectNetwork();

			IServiceAddress rootLeader = pathInfo.RootLeader;

			// Check machine is in the schema,
			MachineProfile machine_p = CheckMachineInNetwork(rootLeader);
			// Check it's root,
			if (!machine_p.IsRoot) {
				throw new NetworkAdminException("Machine '" + rootLeader + "' is not a root");
			}

			// Perform the command,
			RequestMessage request = new RequestMessage("getPathStats");
			request.Arguments.Add(pathInfo.PathName);
			request.Arguments.Add(pathInfo.Version);

			Message m = Command(rootLeader, ServiceType.Root, request);
			if (m.HasError) {
				throw new NetworkAdminException(m.ErrorMessage);
			}

			// Return the stats string for this path
			return m.Arguments[0].ToString();
		}

		public string [] GetPathNames() {
			InspectNetwork();
			// The list of all paths,
			return (string[])SendManagerFunction("getAllPaths");
		}

		public PathInfo GetPathInfo(string pathName) {
			// Query the manager cluster for the PathInfo
			return (PathInfo) SendManagerFunction("getPathInfoForPath", pathName);
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