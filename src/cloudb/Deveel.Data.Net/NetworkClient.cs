using System;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public class NetworkClient {
		private IServiceConnector connector;
		private ServiceStatusTracker serviceTracker;
		private readonly IServiceAddress[] managerAddresses;
		private bool connected;

		private NetworkTreeSystem treeSystem;
		private readonly INetworkCache localNetworkCache;


		private long maximumTransactionNodeCacheHeapSize;

		public NetworkClient(IServiceAddress[] managerAddresses, IServiceConnector connector)
			: this(managerAddresses, connector, MachineState.GetCacheForManager(managerAddresses)) {
		}

		public NetworkClient(IServiceAddress managerAddress, IServiceConnector connector)
			: this(new IServiceAddress[] { managerAddress}, connector) {
		}

		public NetworkClient(IServiceAddress[] managerAddresses, IServiceConnector connector, INetworkCache lnc) {
			this.connector = connector;
			this.managerAddresses = managerAddresses;
			this.localNetworkCache = lnc;
			// Default values,
			MaxTransactionNodeCacheHeapSize = 14*1024*1024;
		}

		public NetworkClient(IServiceAddress managerAddress, ServiceConnector connector, INetworkCache networkCache)
			: this(new IServiceAddress[] { managerAddress}, connector, networkCache) {
		}

		public long MaxTransactionNodeCacheHeapSize {
			get { return maximumTransactionNodeCacheHeapSize; }
			set { maximumTransactionNodeCacheHeapSize = value; }
		}

		public bool IsConnected {
			get { return connected; }
		}

		public void ConnectNetwork() {
			if (IsConnected)
				throw new ApplicationException("Already connected");

			serviceTracker = new ServiceStatusTracker(connector);
			treeSystem = new NetworkTreeSystem(connector, managerAddresses, localNetworkCache, serviceTracker);
			treeSystem.NodeHeapMaxSize = MaxTransactionNodeCacheHeapSize;
			connected = true;
		}

		public void Disconnect() {
			if (connector != null) {
				try {
					connector.Close();
				} finally {
					try {
						serviceTracker.Stop();
					} finally {
						connector = null;
						serviceTracker = null;
						treeSystem = null;
					}

					connected = false;
				}
			}
		}

		public TreeReportNode CreateDiagnosticGraph(ITransaction t) {
			return treeSystem.CreateDiagnosticGraph(t);
		}

		public DataAddress CreateDatabase() {
			try {
				return treeSystem.CreateDatabase();
			} catch (IOException e) {
				throw new NetworkWriteException(e.Message, e);
			}
		}

		public string[] QueryAllNetworkPaths() {
			return treeSystem.FindAllPaths();
		}

		public string GetPathType(string pathName) {
			return treeSystem.GetPathType(pathName);
		}

		public DataAddress GetCurrentSnapshot(string pathName) {
			return treeSystem.GetPathNow(pathName);
		}

		public ITransaction CreateTransaction(DataAddress rootNode) {
			// Create the transaction object and return it,
			return treeSystem.CreateTransaction(rootNode);
		}

		public ITransaction CreateTransaction() {
			// Create the transaction object and return it,
			return treeSystem.CreateTransaction();
		}

		public DataAddress FlushTransaction(ITransaction transaction) {
			return treeSystem.FlushTransaction(transaction);
		}

		public DataAddress Commit(string pathName, DataAddress proposal) {
			return treeSystem.PerformCommit(pathName, proposal);
		}

		public void DisposeTransaction(ITransaction transaction) {
			try {
				treeSystem.DisposeTransaction(transaction);
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		public DataAddress[] GetHistoricalSnapshots(string pathName, DateTime timeStart, DateTime timeEnd) {
			// Return the historical root nodes
			return treeSystem.GetPathHistorical(pathName, DateTimeUtil.GetMillis(timeStart), DateTimeUtil.GetMillis(timeEnd));
		}
	}
}