using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface IPathAccessVerifier : IConfigurable, IRequiresProviderContext {
		bool CanAccess(string pathName, IHttpContext httpContext, OAuthRequestContext requestContext);
	}
}