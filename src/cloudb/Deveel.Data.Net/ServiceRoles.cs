using System;

namespace Deveel.Data.Net {
	[Flags]
	public enum ServiceRoles {
		Manager = 0x01,
		Root = 0x02,
		Block = 0x04
	}
}