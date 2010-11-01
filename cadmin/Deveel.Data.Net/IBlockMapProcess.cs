using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	interface IBlockMapProcess {
		void ManagerProcess(long blockId, IList<long> managerSguids, IList<long> actualSguids,
							IDictionary<long, MachineProfile> sguidToAddress);

		void Process(long blockId, IList<long> availableSguidsContainingBlockId, long[] availableBlockServers,
					 long minThreshold, IDictionary<long, MachineProfile> sguidToAddress);

	}
}