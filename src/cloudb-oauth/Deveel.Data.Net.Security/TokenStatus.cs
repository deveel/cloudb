using System;

namespace Deveel.Data.Net.Security {
	public enum TokenStatus {
		Unknown,
		Unauthorized,
		Authorized,
		Used,
		Expired,
		Revoked
	}
}