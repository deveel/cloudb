using System;

namespace Deveel.Data.Net.Security {
	public enum AuthenticationCode {
		None = -1,
		Success = 1,
		UnknownMechanism = 31,
		SystemError = 101,
		ConnectionProblem = 204,
	}
}