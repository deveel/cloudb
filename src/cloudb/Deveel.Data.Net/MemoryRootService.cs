using System;

namespace Deveel.Data.Net {
	public sealed class MemoryRootService : RootService {
		public MemoryRootService(IServiceConnector connector, IServiceAddress address) 
			: base(connector, address) {
		}
	}
}