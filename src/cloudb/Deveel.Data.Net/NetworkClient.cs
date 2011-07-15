using System;
using System.IO;

using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net {
	public class NetworkClient : IDisposable {
		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector)
			: this(managerAddress, connector, ManagerCacheState.GetCache(managerAddress)) {
		}

		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector, INetworkCache cache) {
			if (!(connector.MessageSerializer is IRpcMessageSerializer) || 
				!((IRpcMessageSerializer)connector.MessageSerializer).SupportsMessageStream)
				throw new ArgumentException("The connector given has an invalid message serializer for this context (must be a IRPC serializer).");
			
			this.connector = connector;
			this.managerAddress = managerAddress;
			this.cache = cache;
		}

		private IServiceConnector connector;
		private readonly IServiceAddress managerAddress;
		private NetworkTreeSystem storageSystem;
		private readonly INetworkCache cache;
		private bool connected;

		private long maxTransactionNodeCacheHeapSize;

		public long MaxTransactionNodeCacheHeapSize {
			get { return maxTransactionNodeCacheHeapSize; }
			set { maxTransactionNodeCacheHeapSize = value; }
		}
		
		public bool IsConnected {
			get { return connected; }
		}
		
		private void CheckConnected() {
			if (!connected)
				throw new InvalidOperationException("The client is not connected.");
		}

		public void Connect() {
			if (connected)
				throw new InvalidOperationException("The client is already connected.");
			
			storageSystem = new NetworkTreeSystem(connector, managerAddress, cache);
			storageSystem.SetMaxNodeCacheHeapSize(maxTransactionNodeCacheHeapSize);
			connected = true;
		}

		private void OnDisconnected() {
			connector = null;
			storageSystem = null;
		}

		internal void Disconnect() {
			CheckConnected();
			
			if (connector != null)
				connector.Close();

			OnDisconnected();

			connected = false;
		}

		public DataAddress CreateEmptyDatabase() {
			CheckConnected();
			
			try {
				return storageSystem.CreateEmptyDatabase();
			} catch (IOException e) {
				throw new NetworkWriteException(e.Message, e);
			}
		}

		public ITransaction CreateEmptyTransaction() {
			CheckConnected();
			
			// Create the transaction object and return it,
			return storageSystem.CreateEmptyTransaction();
		}

		public ITransaction CreateTransaction(DataAddress rootNode) {
			CheckConnected();
			
			return storageSystem.CreateTransaction(rootNode);
		}

		public DataAddress FlushTransaction(ITransaction transaction) {
			CheckConnected();
			
			return storageSystem.FlushTransaction(transaction);
		}

		public DataAddress Commit(string pathName, DataAddress proposal) {
			CheckConnected();
			
			IServiceAddress rootAddress = storageSystem.GetRootServer(pathName);
			return storageSystem.Commit(rootAddress, pathName, proposal);
		}

		public void DisposeTransaction(ITransaction transaction) {
			CheckConnected();
			
			try {
				storageSystem.DisposeTransaction(transaction);
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

		public DataAddress[] GetHistoricalSnapshots(string pathName, DateTime timeStart, DateTime timeEnd) {
			CheckConnected();
			
			IServiceAddress rootAddress = storageSystem.GetRootServer(pathName);
			return storageSystem.GetSnapshots(rootAddress, pathName, timeStart, timeEnd);
		}

		public DataAddress GetCurrentSnapshot(string pathName) {
			CheckConnected();
			
			IServiceAddress rootAddress = storageSystem.GetRootServer(pathName);

			if (rootAddress == null)
				throw new Exception("There are no root servers in the network for path '" + pathName + "'");

			return storageSystem.GetSnapshot(rootAddress, pathName);
		}

		public string[] GetNetworkPaths() {
			CheckConnected();
			
			return storageSystem.FindAllPaths();
		}

		public TreeGraph CreateDiagnosticGraph(ITransaction t) {
			CheckConnected();
			
			return storageSystem.CreateDiagnosticGraph(t);
		}

		public void CreateReachabilityList(TextWriter warning_log, DataAddress root_node, IIndex discovered_node_list) {
			CheckConnected();
			
			try {
				storageSystem.CreateReachabilityList(warning_log, root_node.Value, discovered_node_list);
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			Disconnect();
		}

		#endregion
	}
}