using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public interface IHttpContext {
		IHttpRequest Request { get; }

		IHttpResponse Response { get; }

		IPrincipal User { get; set; }
	}
}