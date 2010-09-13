using System;
using System.IO;

namespace Deveel.Data.Net {
	public class NetworkClient : IDisposable {
		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector)
			: this(managerAddress, connector, ManagerCacheState.GetCache(managerAddress)) {
		}

		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector, INetworkCache cache) {
			this.connector = connector;
			this.managerAddress = managerAddress;
			this.cache = cache;
		}

		private IServiceConnector connector;
		private readonly IServiceAddress managerAddress;
		private NetworkTreeSystem storageSystem;
		private readonly INetworkCache cache;

		private long maxTransactionNodeCacheHeapSize;

		public long MaxTransactionNodeCacheHeapSize {
			get { return maxTransactionNodeCacheHeapSize; }
			set { maxTransactionNodeCacheHeapSize = value; }
		}

		public void Connect() {
			storageSystem = new NetworkTreeSystem(connector, managerAddress, cache);
			storageSystem.SetMaxNodeCacheHeapSize(maxTransactionNodeCacheHeapSize);
		}

		private void OnDisconnected() {
			connector = null;
			storageSystem = null;
		}

		internal void Disconnect() {
			if (connector != null)
				connector.Close();

			OnDisconnected();
		}

		public DataAddress CreateEmptyDatabase() {
			try {
				return storageSystem.CreateEmptyDatabase();
			} catch (IOException e) {
				throw new NetworkWriteException(e.Message, e);
			}
		}

		public ITransaction CreateEmptyTransaction() {
			// Create the transaction object and return it,
			return storageSystem.CreateEmptyTransaction();
		}

		public ITransaction CreateTransaction(DataAddress rootNode) {
			return storageSystem.CreateTransaction(rootNode);
		}

		public DataAddress FlushTransaction(ITransaction transaction) {
			return storageSystem.FlushTransaction(transaction);
		}

		public DataAddress Commit(string pathName, DataAddress proposal) {
			IServiceAddress rootAddress = storageSystem.GetRootServer(pathName);
			return storageSystem.Commit(rootAddress, pathName, proposal);
		}

		public void DisposeTransaction(ITransaction transaction) {
			try {
				storageSystem.DisposeTransaction(transaction);
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

		public DataAddress[] GetHistoricalSnapshots(string pathName, DateTime timeStart, DateTime timeEnd) {
			IServiceAddress rootAddress = storageSystem.GetRootServer(pathName);
			return storageSystem.GetPathHistorical(rootAddress, pathName, timeStart, timeEnd);
		}

		public DataAddress GetCurrentSnapshot(string pathName) {
			IServiceAddress rootAddress = storageSystem.GetRootServer(pathName);

			if (rootAddress == null)
				throw new Exception("There are no root servers in the network for path '" + pathName + "'");

			return storageSystem.GetPathNow(rootAddress, pathName);
		}

		public string[] GetNetworkPaths() {
			return storageSystem.FindAllPaths();
		}

		public TreeGraph CreateDiagnosticGraph(ITransaction t) {
			return storageSystem.CreateDiagnosticGraph(t);
		}

		public void CreateReachabilityList(TextWriter warning_log, DataAddress root_node, IIndex discovered_node_list) {
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