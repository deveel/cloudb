using System;

namespace Deveel.Data.Net {
	[Flags]
	public enum ServiceType {
		Admin = 0x00,
		Root = 0x02,
		Block = 0x04,
		Manager = 0x01
	}
}