using System;

namespace Deveel.Data.Net.Security {
	public interface IResourceAccessVerifier {
		bool VerifyAccess(IHttpContext httpContext, OAuthRequestContext context);
	}
}