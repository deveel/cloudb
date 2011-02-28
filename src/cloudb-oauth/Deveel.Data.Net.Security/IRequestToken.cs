using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public interface IRequestToken : IIssuedToken {
		OAuthParameters AssociatedParameters { get; }

		IIdentity AuthenticatedUser { get; }

		string[] Roles { get; }
	}
}