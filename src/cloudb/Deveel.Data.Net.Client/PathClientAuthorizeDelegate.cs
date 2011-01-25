using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Client {
	public delegate bool PathClienAuthorizeDelegate(IIdentity identity);
}