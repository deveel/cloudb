using System;

namespace Deveel.Data.Net {
	public enum ServiceStatus {
		Up = 1,
		DownShutdown = 2,
		DownHeartbeat = 3,
		DownClientReport = 4
	}
}