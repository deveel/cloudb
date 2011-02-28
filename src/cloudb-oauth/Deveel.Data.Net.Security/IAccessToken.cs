using System;

namespace Deveel.Data.Net.Security {
	public interface IAccessToken : IIssuedToken {
		IRequestToken RequestToken { get; }
	}
}