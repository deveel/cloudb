using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class MemoryManagerService : ManagerService {
		private MemoryDatabase database;

		public MemoryManagerService(IServiceConnector connector, IServiceAddress address) 
			: base(connector, address) {
		}

		protected override void OnStart() {
			database = new MemoryDatabase(1024);
			database.Start();

			SetBlockDatabase(database);
		}

		protected override void OnStop() {
			base.OnStop();

			database.Stop();
			database = null;
		}

		protected override void PersistBlockServers(IList<BlockServiceInfo> serviceList) {
		}

		protected override void PersistRootServers(IList<RootServiceInfo> serviceList) {
		}

		protected override void PersistManagerServers(IList<ManagerServiceInfo> serversList) {
		}

		protected override void PersistManagerUniqueId(int uniqueId) {
		}
	}
}