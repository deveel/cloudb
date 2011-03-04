using System;

namespace Deveel.Data.Net.Security {
	public interface IAuthorizationHandler {
		bool Authorize(IToken requestToken);
	}
}