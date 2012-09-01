using System;

namespace Deveel.Data.Net {
	[Flags]
	public enum MachineRoles {
		None = 0,
		Manager = 0x01,
		Root = 0x02,
		Block = 0x04
	}
}