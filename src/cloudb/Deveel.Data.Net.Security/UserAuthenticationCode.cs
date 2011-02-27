using System;

namespace Deveel.Data.Net.Security {
	public enum UserAuthenticationCode {
		Success = 1,
		UserNotFound = 2,
		InvalidPassword = 3,
		UnknownError = 4
	}
}