using System;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public abstract class RootServer {
		private ErrorStateException errorState;
		
		protected abstract Stream CreatePathAccessStream(string path);
		
		private void SetErrorState(Exception e) {
			errorState = new ErrorStateException(e);
		}
		
		private void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}
		
		private void CheckPathType(string pathTypeName) {
			throw new NotImplementedException();
		}
		
		private void AddPath(string pathName, string pathTypeName, DataAddress rootNode) {
			throw new NotImplementedException();
		}
		
		private void RemovePath(string pathName) {
			throw new NotImplementedException();
		}
		
		private string GetPathType(string pathName) {
			throw new NotImplementedException();
		}
		
		private void InitPath(string pathName) {
			throw new NotImplementedException();
		}
		
		private void PublishPath(string name, DataAddress rootNode) {
			throw new NotImplementedException();
		}
		
		private DataAddress GetSnapshot(string pathName) {
			throw new NotImplementedException();
		}
		
		private DataAddress[] GetSnapshots(string pathName, DateTime start, DateTime end) {
			throw new NotImplementedException();
		}
		
		private DataAddress[] GetSnapshots(string pathName, DateTime start) {
			throw new NotImplementedException();
		}
		
		private DataAddress Commit(String pathName, DataAddress proposal) {
			throw new NotImplementedException();
		}
		
		protected abstract void BindWithManager(ServiceAddress managerAddress);
		
		protected abstract void UnbindWithManager(ServiceAddress managerAddress);
		
		#region PathAccess
		
		class PathAccess {
			private readonly string name;
			private readonly string pathTypeName;
			private IPath path;
			private readonly Stream accessStream;
			private readonly StrongPagedAccess pagedAccess;
			private DataAddress lastDataAddress;

			public byte[] tempBuffer = new byte[32];
			
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
			
			public DataAddress LastDataAddress {
				get { return lastDataAddress; }
				set { lastDataAddress = value; }
			}
			
			public Stream AccessStream {
				get { return accessStream; }
			}
			
			public StrongPagedAccess PagedAccess {
				get { return pagedAccess; }
			}
		}
		
		#endregion
		
		#region RootServerProcessor
		
		class RootServerMessageProcessor : IMessageProcessor {
			public RootServerMessageProcessor(RootServer server) {
				this.server = server;
			}
			
			private readonly RootServer server;			
			
			public MessageStream Process(MessageStream messageStream) {
				// The reply message,
				MessageStream responseStream = new MessageStream(32);

				// The messages input the stream,
				foreach (Message m in messageStream) {
					try {
						server.CheckErrorState();
						
						string messageName = m.Name;
						switch(messageName) {
							case "publishPath": {
								server.PublishPath((string)m[0], (DataAddress)m[1]);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "getSnapshot": {
								string path = (string)m[0];
								DataAddress address = server.GetSnapshot(path);
								responseStream.AddMessage("R", address);
								break;
							}
							case "getSnapshots": {
								string path = (string)m[0];
								DateTime start = DateTime.FromBinary((long)m[1]);
								DateTime end = DateTime.FromBinary((long)m[2]);
								DataAddress[] addresses = server.GetSnapshots(path, start, end);
								responseStream.AddMessage("R", addresses);
								break;
							}
							case "getCurrentTyime": {
								responseStream.AddMessage("R", DateTime.Now.ToUniversalTime().ToBinary());
								break;	
							}
							case "addPath": {
								string pathName = (string) m[0];
								string pathTypeName = (string)m[1];
								DataAddress rootNode = (DataAddress)m[2];
								server.AddPath(pathName, pathTypeName, rootNode);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "removePath": {
								string pathName = (string)m[0];
								server.RemovePath(pathName);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "getPathType": {
								string pathName = (string)m[0];
								string pathType = server.GetPathType(pathName);
								responseStream.AddMessage("R", pathType);
								break;
							}
							case "checkPathType": {
								string pathType = (string)m[0];
								server.CheckPathType(pathType);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "initPath": {
								string pathName = (string) m[0];
								server.InitPath(pathName);
								responseStream.AddMessage("R", 1);
								break;	
							}
							case "commit": {
								string pathName = (string) m[0];
								DataAddress proposal = (DataAddress)m[1];
								DataAddress rootNode = server.Commit(pathName, proposal);
								responseStream.AddMessage("R", rootNode);
								break;
							}
							case "bindWithManager": {
								ServiceAddress manager = (ServiceAddress)m[0];
								server.BindWithManager(manager);
								responseStream.AddMessage("R", 1);
								break;
							}
							case "unbindWithManager": {
								ServiceAddress manager = (ServiceAddress)m[0];
								server.UnbindWithManager(manager);
								responseStream.AddMessage("R", 1);
								break;
							}
							default:
								throw new ApplicationException("Unknown message received: " + messageName);
						}
					} catch (OutOfMemoryException e) {
						//TODO: ERROR log ...
						server.SetErrorState(e);
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
			private readonly RootServer server;
			private readonly string pathName;
			private readonly NetworkTreeSystem treeSystem;
			
			public PathConnection(RootServer server, string pathName, 
			                      IServiceConnector connector, ServiceAddress manager, 
			                      INetworkCache networkCache) {
				this.server = server;
				this.pathName = pathName;
				treeSystem = new NetworkTreeSystem(connector, manager, networkCache);
			}
			
			public DataAddress GetSnapshot() {
				try {
					return server.GetSnapshot(pathName);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public DataAddress[] GetSnapshots(DateTime start, DateTime end) {
				try {
					return server.GetSnapshots(pathName, start, end);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public DataAddress[] GetSnapshots(DateTime start) {
				try {
					return server.GetSnapshots(pathName, start);
				} catch(IOException e) {
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
			
			public void Publish(DataAddress rootNode) {
				try {
					server.PublishPath(pathName, rootNode);
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
	}
}