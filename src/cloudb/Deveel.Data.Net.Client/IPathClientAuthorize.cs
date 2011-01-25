using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Client {
	public interface IPathClientAuthorize {
		bool IsAuthorized(IIdentity identity);
	}
}