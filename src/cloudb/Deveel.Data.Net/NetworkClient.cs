using System;
using System.IO;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public class NetworkClient : IDisposable {
		public NetworkClient(IServiceAddress[] managerAddresses, IServiceConnector connector)
			: this(managerAddresses, connector, ManagerCacheState.GetCache(managerAddresses)) {
		}

		public NetworkClient(IServiceAddress[] managerAddresses, IServiceConnector connector, INetworkCache cache) {
			if (!(connector.MessageSerializer is IMessageStreamSupport))
				throw new ArgumentException("The connector given has an invalid message serializer for this context (must be a IRPC serializer).");
			
			this.connector = connector;
			this.managerAddresses = managerAddresses;
			this.cache = cache;
		}

		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector)
			: this(new IServiceAddress[] { managerAddress }, connector, ManagerCacheState.GetCache(new IServiceAddress[] { managerAddress })) {
		}

		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector, INetworkCache cache)
			: this(new IServiceAddress[] { managerAddress }, connector, cache) {
		}


		private IServiceConnector connector;
		private ServiceStatusTracker serviceTracker;
		private readonly IServiceAddress[] managerAddresses;
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

			serviceTracker = new ServiceStatusTracker(connector);
			storageSystem = new NetworkTreeSystem(connector, managerAddresses, cache, serviceTracker);
			storageSystem.SetMaxNodeCacheHeapSize(maxTransactionNodeCacheHeapSize);
			connected = true;
		}

		private void OnDisconnected() {
			connector = null;
			storageSystem = null;
		}

		internal void Disconnect() {
			CheckConnected();

			try {
				if (connector != null)
					connector.Close();
			} finally {
				try {
					serviceTracker.Stop();

					OnDisconnected();
				} finally {
					serviceTracker = null;
					connected = false;	

					OnDisconnected();
				}
			}
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
			return storageSystem.Commit(pathName, proposal);
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
			return storageSystem.GetSnapshots(pathName, timeStart, timeEnd);
		}

		public DataAddress GetCurrentSnapshot(string pathName) {
			CheckConnected();

			return storageSystem.GetSnapshot(pathName);
		}

		public string[] GetNetworkPaths() {
			CheckConnected();
			
			return storageSystem.FindAllPaths();
		}

		public TreeGraph CreateDiagnosticGraph(ITransaction t) {
			CheckConnected();
			
			return storageSystem.CreateDiagnosticGraph(t);
		}

		public void CreateReachabilityList(TextWriter warningLog, DataAddress rootNode, IIndex discovered_node_list) {
			CheckConnected();
			
			try {
				storageSystem.CreateReachabilityList(warningLog, rootNode.Value, discovered_node_list);
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