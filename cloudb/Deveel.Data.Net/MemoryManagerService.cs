using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class MemoryManagerService : ManagerService {
		private MemoryDatabase database;

		public MemoryManagerService(IServiceConnector connector, ServiceAddress address) 
			: base(connector, address) {
		}

		protected override void OnInit() {
			database = new MemoryDatabase(1024);
			database.Start();

			SetBlockDatabase(database);
		}

		protected override void OnDispose(bool disposing) {
			base.OnDispose(disposing);

			if (disposing) {
				database.Stop();
				database = null;
			}
		}

		protected override void PersistBlockServers(IList<BlockServerInfo> servers_list) {
		}

		protected override void PersistRootServers(IList<RootServerInfo> servers_list) {
		}
	}
}