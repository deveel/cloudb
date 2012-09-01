using System;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public abstract class NetworkClient {
		private IServiceConnector networkConnector;
		private ServiceStatusTracker serviceTracker;
		private readonly IServiceAddress[] managerAddresses;

		private NetworkTreeSystem treeSystem;
		private readonly INetworkCache localNetworkCache;


		private long maximumTransactionNodeCacheHeapSize;

		internal NetworkClient(IServiceAddress[] managerServers, IServiceConnector connector)
			: this(managerServers, connector, MachineState.GetCacheForManager(managerServers)) {
		}

		internal NetworkClient(IServiceAddress[] managerServers, IServiceConnector connector, INetworkCache lnc) {
			this.networkConnector = connector;
			this.managerAddresses = managerServers;
			this.localNetworkCache = lnc;
			// Default values,
			MaxTransactionNodeCacheHeapSize = 14*1024*1024;
		}

		public long MaxTransactionNodeCacheHeapSize {
			get { return maximumTransactionNodeCacheHeapSize; }
			set { maximumTransactionNodeCacheHeapSize = value; }
		}

		internal void ConnectNetwork() {
			if (networkConnector != null) {
				throw new ApplicationException("Already connected");
			}

			serviceTracker = new ServiceStatusTracker(networkConnector);
			treeSystem = new NetworkTreeSystem(networkConnector, managerAddresses, localNetworkCache, serviceTracker);
			treeSystem.NodeHeapMaxSize = MaxTransactionNodeCacheHeapSize;
		}

		internal void Disconnect() {
			if (networkConnector != null) {
				try {
					networkConnector.Close();
				} finally {
					try {
						serviceTracker.Stop();
					} finally {
						networkConnector = null;
						serviceTracker = null;
						treeSystem = null;
					}
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