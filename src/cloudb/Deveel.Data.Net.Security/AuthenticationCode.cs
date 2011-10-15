using System;

namespace Deveel.Data.Net.Security {
	public enum AuthenticationCode {
		None = -1,
		Success = 1,
		NeedMoreData = 22,
		MissingData = 24,
		UnknownMechanism = 31,
		SystemError = 101,
		ConnectionProblem = 204,
	}
}