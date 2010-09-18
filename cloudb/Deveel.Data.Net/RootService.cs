using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public abstract class RootService : Service {
		protected RootService(IServiceConnector connector) {
			this.connector = connector;
			pathCache = new Dictionary<string, PathAccess>(128);
		}		
		
		private IServiceAddress managerAddress;
		private ErrorStateException errorState;
		private readonly IServiceConnector connector;
		private readonly Dictionary<string, PathAccess> pathCache;
		
		public override ServiceType ServiceType {
			get { return ServiceType.Root; }
		}
		
		protected IServiceAddress ManagerAddress {
			get { return managerAddress; }
			set { managerAddress = value; }
		}
						
		private long BinarySearch(StrongPagedAccess access, long low, long high, long timestamp) {
			while (low <= high) {
				long mid = (low + high) >> 1;

				long midTimeStamp = access.ReadInt64(mid * 16);

				if (midTimeStamp < timestamp) {
					low = mid + 1;
				} else if (midTimeStamp > timestamp) {
					high = mid - 1;
				} else {
					return mid;
				}
			}
			return -(low + 1);
		}
		
		private void CheckPathNameValid(string name) {
			int sz = name.Length;
			for (int i = 0; i < sz; ++i) {
				char c = name[i];
				// If the character is not a letter or digit or lower case, then it's
				// invalid
				if (!Char.IsLetterOrDigit(c) || Char.IsUpper(c))
					throw new ApplicationException("Path name '" + name + "' is invalid, must contain only lower case letters or digits.");
			}
		}
		
		private void CheckPathType(string pathTypeName) {
			try {
				Type type = Type.GetType(pathTypeName, true, true);
				Activator.CreateInstance(type);
			} catch (TypeLoadException e) {
				throw new ApplicationException("Type not found: " + e.Message);
			} catch (TypeInitializationException e) {
				throw new ApplicationException("Type instantiation exception: " + e.Message);
			} catch (UnauthorizedAccessException e) {
				throw new ApplicationException("Unauthorized Access exception: " + e.Message);
			}
		}
		
		private void AddPath(string pathName, string pathTypeName, DataAddress rootNode) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			lock (pathCache) {
				if (pathCache.ContainsKey(pathName))
					// If it's input the local map, generate an error,
					throw new ApplicationException("Path '" + pathName + "' already exists.");
				
				CreatePath(pathName, pathTypeName);
			}

			if (rootNode != null) {
				// Finally publish the base_root to the path,
				PublishPath(pathName, rootNode);
			}
		}
		
		private void RemovePath(string pathName) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			lock (pathCache) {
				DeletePath(pathName);
				pathCache.Remove(pathName);
			}
		}
		
		private string GetPathType(string pathName) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			PathAccess pathAccess = GetPathAccess(pathName);
			return pathAccess.PathTypeName;
		}

		private void PathReport(out string[] pathNames, out string[] pathTypes) {
			IList<PathStatus> pathStatuses;

			lock(pathCache) {
				pathStatuses = ListPaths();
			}

			List<string> pathNamesList = new List<string>();
			foreach(PathStatus pathStatus in pathStatuses) {
				if (!pathStatus.IsDeleted)
					pathNamesList.Add(pathStatus.PathName);
			}

			pathNames = pathNamesList.ToArray();
			pathTypes = new string[pathNames.Length];

			for (int i = 0; i < pathNames.Length; i++) {
				PathAccess pathAccess = GetPathAccess(pathNames[i]);
				pathTypes[i] = pathAccess.PathTypeName;
			}
		}
		
		private void InitPath(string pathName) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathName);

			IPath path;
			lock (pathAccess) {
				try {
					path = pathAccess.Path;
				} catch (TypeLoadException e) {
					throw new ApplicationException("Type not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new ApplicationException("Type instantiation exception: " + e.Message);
				} catch (UnauthorizedAccessException e) {
					throw new ApplicationException("Unauthorized Access exception: " + e.Message);
				}
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache networkCache = ManagerCacheState.GetCache(managerAddress);
			IPathConnection connection = new PathConnection(this, pathName, connector, managerAddress, networkCache);

			// Call the initialize function,
			path.Init(connection);
		}
		
		private void PublishPath(string name, DataAddress rootNode) {
			// Check the name given is valid,
			CheckPathNameValid(name);

			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(name);

			lock (pathAccess) {
				// Write the root node entry to the end of the stream,
				Stream f = pathAccess.AccessStream;
				long pos = f.Length;
				f.Seek(pos, SeekOrigin.Begin);
				byte[] buffer = new byte[16];
				ByteBuffer.WriteInt8(DateTime.Now.ToBinary(), buffer, 0);
				ByteBuffer.WriteInt8(rootNode.Value, buffer, 8);
				f.Write(buffer, 0, 16);
				pathAccess.PagedAccess.InvalidateSection(pos, 16);
				pathAccess.LastDataAddress = rootNode;
			}
		}
		
		private DataAddress GetSnapshot(string pathName) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathName);
			
			lock (pathAccess) {
				if (pathAccess.LastDataAddress == null) {
					// Read the root node entry from the end of the file,

					long pos = pathAccess.AccessStream.Length;
					if (pos == 0) {
						// Nothing input the file, so return null
						return null;
					}
					
					// Read the long at the position of the root node reference,
					long rootNodeId = pathAccess.PagedAccess.ReadInt64(pos - 8);
					pathAccess.LastDataAddress = new DataAddress(rootNodeId);
					// Clear the cache if it's over a certain size,
					pathAccess.PagedAccess.ClearCache(4);
				}
				return pathAccess.LastDataAddress;
			}

		}
		
		private DataAddress[] GetSnapshots(string pathName, DateTime start, DateTime end) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathName);

			List<DataAddress> nodes = new List<DataAddress>();
			lock (pathAccess) {
				// We perform a binary search for the start and end time input the set of
				// root nodes since the records are ordered by time. Note that the key
				// is a timestamp obtained by System.currentTimeMillis() that could
				// become output of order if the system time is changed or other
				// misc time synchronization oddities. Because of this, we consider the
				// records 'roughly' ordered and it should be noted the result may not
				// be exactly correct.

				long setSize = pathAccess.AccessStream.Length / 16;

				long startPos = BinarySearch(pathAccess.PagedAccess, 0, setSize - 1, start.ToBinary());
				long endPos = BinarySearch(pathAccess.PagedAccess, 0, setSize - 1, end.ToBinary());
				if (startPos < 0)
					startPos = -(startPos + 1);
				if (endPos < 0)
					endPos = -(endPos + 1);
				
				// Crudely clear the cache if it's reached a certain threshold,
				pathAccess.PagedAccess.ClearCache(4);

				if (startPos >= endPos - 1) {
					startPos = endPos - 2;
					endPos = endPos + 2;
				}

				startPos = Math.Max(0, startPos);
				endPos = Math.Min(setSize, endPos);

				// Return the records,
				while (true) {
					if (startPos > endPos || startPos >= setSize)
						// End if start has reached the end,
						break;
					
					long node = pathAccess.PagedAccess.ReadInt64((startPos * 16) + 8);
					nodes.Add(new DataAddress(node));
					// Crudely clear the cache if it's reached a certain threshold,
					pathAccess.PagedAccess.ClearCache(4);

					++startPos;
				}
			}

			// Return the nodes array,
			return nodes.ToArray();
		}
		
		private DataAddress[] GetSnapshots(string pathName, DataAddress rootNode) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathName);

			List<DataAddress> rootList = new List<DataAddress>(6);
			lock (pathAccess) {
				bool found = false;

				// Start position at the end of the file,
				long pos = pathAccess.AccessStream.Length;

				while (!found && pos > 0) {
					// Iterate backwards,
					pos = pos - 16;
					// Read the root node for this record,
					long rootNodeId = pathAccess.PagedAccess.ReadInt64(pos + 8);
					// Crudely clear the cache if it's reached a certain threshold,
					pathAccess.PagedAccess.ClearCache(4);
					// Did we find the root node?
					DataAddress rootNodeAddress = new DataAddress(rootNodeId);
					if (rootNodeAddress.Equals(rootNode)) {
						found = true;
					} else {
						rootList.Add(rootNodeAddress);
					}
				}

				// If not found, report error
				if (!found) {
					// Bit of an obscure error message. This basically means we didn't
					// find the root node requested input the path file.
					throw new ApplicationException("Root not found input version list");
				}

				// Return the array as a list,
				return rootList.ToArray();

			}
		}
		
		private DataAddress Commit(String pathName, DataAddress proposal) {
			// Check the name given is valid,
			CheckPathNameValid(pathName);

			// Fetch the path access object for the given name. This will generate an
			// exception if the path doesn't exist or there is an error input the
			// configuration of the path.
			PathAccess pathAccess = GetPathAccess(pathName);

			IPath path;
			lock (pathAccess) {
				try {
					path = pathAccess.Path;
				} catch (TypeLoadException e) {
					throw new CommitFaultException("Type not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new CommitFaultException("Type instantiation exception: " + e.Message);
				} catch (UnauthorizedAccessException e) {
					throw new CommitFaultException("Unauthorized Access exception: " + e.Message);
				}
			}

			// Create the connection object (should be fairly lightweight)
			INetworkCache networkCache = ManagerCacheState.GetCache(managerAddress);
			IPathConnection connection = new PathConnection(this, pathName, connector, managerAddress, networkCache);

			// Perform the commit,
			return path.Commit(connection, proposal);
		}
		
		private void BindWithManager(IServiceAddress managerAddress) {
			if (this.managerAddress != null)
				throw new ApplicationException("This root service is already bound to a manager service");
			
			try {
				OnBindingWithManager(managerAddress);
			} catch(Exception e) {
				throw new ApplicationException("Error while binding with manager: " + e.Message, e);
			}
			
			this.managerAddress = managerAddress;
		}
		
		private void UnbindWithManager(IServiceAddress managerAddress) {
			if (this.managerAddress == null)
				throw new ApplicationException("This root service is not bound to a manager service");
			
			if (!this.managerAddress.Equals(managerAddress))
				throw new ApplicationException("Trying to unbind a different manager service");
			
			try {
				OnUnbindingWithManager(managerAddress);
			} catch (Exception e) {
				throw new ApplicationException("Error while unbinding with manager: " + e.Message, e);
			}
			
			this.managerAddress = null;
		}
		
		private PathAccess GetPathAccess(string pathName) {
			// Fetch the lock object for the given path name,
			PathAccess pathAccess;
			lock (pathCache) {
				if (!pathCache.TryGetValue(pathName, out pathAccess)) {
					pathAccess = FetchPathAccess(pathName);
					
					// Put it input the local map
					pathCache[pathName] = pathAccess;
				}
			}
			return pathAccess;
		}

		
		protected virtual void OnBindingWithManager(IServiceAddress managerAddress) {
		}
		
		protected virtual void OnUnbindingWithManager(IServiceAddress managerAddress) {
		}
		
		protected override void OnDispose(bool disposing) {
			if (disposing)
				managerAddress = null;
		}

		protected override IMessageProcessor CreateProcessor() {
			return new RootServerMessageProcessor(this);
		}
						
		protected abstract PathAccess FetchPathAccess(string pathName);
		
		protected abstract void CreatePath(string pathName, string pathTypeName);
		
		protected abstract void DeletePath(string pathName);

		protected abstract IList<PathStatus> ListPaths();
				
		#region PathAccess
		
		protected class PathAccess {
			private readonly string name;
			private readonly string pathTypeName;
			private IPath path;
			private readonly Stream accessStream;
			private readonly StrongPagedAccess pagedAccess;
			private DataAddress lastDataAddress;
			
			public PathAccess(Stream accessStream, string name, string pathTypeName) {
				this.name = name;
				this.pathTypeName = pathTypeName;
				this.accessStream = accessStream;
				pagedAccess = new StrongPagedAccess(accessStream, 1024);
			}
			
			public string Name {
				get { return name; }
			}
			
			public IPath Path {
				get {
					if (path == null) {
						Type pathType = Type.GetType(pathTypeName, true, true);
						path = (IPath)Activator.CreateInstance(pathType);
					}
					return path;
				}
			}
			
			public string PathTypeName {
				get { return pathTypeName; }
			}
			
			internal DataAddress LastDataAddress {
				get { return lastDataAddress; }
				set { lastDataAddress = value; }
			}
			
			public Stream AccessStream {
				get { return accessStream; }
			}
			
			internal StrongPagedAccess PagedAccess {
				get { return pagedAccess; }
			}
		}
		
		#endregion
		
		#region RootServerProcessor
		
		class RootServerMessageProcessor : IMessageProcessor {
			public RootServerMessageProcessor(RootService service) {
				this.service = service;
			}
			
			private readonly RootService service;			
			
			public MessageStream Process(MessageStream messageStream) {
				// The reply message,
				MessageStream responseStream = new MessageStream(32);

				// The messages input the stream,
				foreach (Message m in messageStream) {
					try {
						service.CheckErrorState();
						
						string messageName = m.Name;
						switch(messageName) {
							case "publishPath": {
								service.PublishPath((string)m[0], (DataAddress)m[1]);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "getSnapshot": {
								string path = (string)m[0];
								DataAddress address = service.GetSnapshot(path);
								responseStream.AddMessage("R", address);
								break;
							}
							case "getSnapshots": {
								string path = (string)m[0];
								DateTime start = DateTime.FromBinary((long)m[1]);
								DateTime end = DateTime.FromBinary((long)m[2]);
								DataAddress[] addresses = service.GetSnapshots(path, start, end);
								responseStream.AddMessage("R", addresses);
								break;
							}
							case "getCurrentTime": {
								responseStream.AddMessage("R", DateTime.Now.ToUniversalTime().ToBinary());
								break;	
							}
							case "addPath": {
								string pathName = (string) m[0];
								string pathTypeName = (string)m[1];
								DataAddress rootNode = (DataAddress)m[2];
								service.AddPath(pathName, pathTypeName, rootNode);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "removePath": {
								string pathName = (string)m[0];
								service.RemovePath(pathName);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "getPathType": {
								string pathName = (string)m[0];
								string pathType = service.GetPathType(pathName);
								responseStream.AddMessage("R", pathType);
								break;
							}
							case "checkPathType": {
								string pathType = (string)m[0];
								service.CheckPathType(pathType);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "initPath": {
								string pathName = (string) m[0];
								service.InitPath(pathName);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "commit": {
								string pathName = (string) m[0];
								DataAddress proposal = (DataAddress)m[1];
								DataAddress rootNode = service.Commit(pathName, proposal);
								responseStream.AddMessage("R", rootNode);
								break;
							}
							case "bindWithManager": {
								IServiceAddress manager = (IServiceAddress)m[0];
								service.BindWithManager(manager);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "unbindWithManager": {
								IServiceAddress manager = (IServiceAddress)m[0];
								service.UnbindWithManager(manager);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "pathReport": {
								string[] pathNames, pathTypes;
								service.PathReport(out pathNames, out pathTypes);
								responseStream.AddMessage("R", pathNames, pathTypes);
								break;
							}
							default:
								throw new ApplicationException("Unknown message received: " + messageName);
						}
					} catch (OutOfMemoryException e) {
						//TODO: ERROR log ...
						service.SetErrorState(e);
						throw e;
					} catch (Exception e) {
						//TODO: ERROR log ...
						responseStream.AddErrorMessage(new ServiceException(e));
					}
				}

				return responseStream;
			}
		} 

		#endregion
		
		#region PathConnection
		
		class PathConnection : IPathConnection {
			private readonly RootService service;
			private readonly string pathName;
			private readonly NetworkTreeSystem treeSystem;
			
			public PathConnection(RootService service, string pathName, 
			                      IServiceConnector connector, IServiceAddress manager, 
			                      INetworkCache networkCache) {
				this.service = service;
				this.pathName = pathName;
				treeSystem = new NetworkTreeSystem(connector, manager, networkCache);
			}
			
			public DataAddress GetSnapshot() {
				try {
					return service.GetSnapshot(pathName);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public DataAddress[] GetSnapshots(DateTime start, DateTime end) {
				try {
					return service.GetSnapshots(pathName, start, end);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public DataAddress[] GetSnapshots(DataAddress rootNode) {
				try {
					return service.GetSnapshots(pathName, rootNode);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public void Publish(DataAddress rootNode) {
				try {
					service.PublishPath(pathName, rootNode);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public ITransaction CreateTransaction(DataAddress rootNode) {
				return treeSystem.CreateTransaction(rootNode);
			}
			
			public DataAddress CommitTransaction(ITransaction transaction) {
				return treeSystem.FlushTransaction(transaction);
			}
		}
		
		#endregion

		#region PathStatus

		protected sealed class PathStatus {
			public PathStatus(string pathName, bool deleted) {
				this.pathName = pathName;
				this.deleted = deleted;
			}

			private readonly string pathName;
			private readonly bool deleted;

			public bool IsDeleted {
				get { return deleted; }
			}

			public string PathName {
				get { return pathName; }
			}
		}

		#endregion
	}
}