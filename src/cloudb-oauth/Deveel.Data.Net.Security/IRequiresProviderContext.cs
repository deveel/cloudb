using System;

namespace Deveel.Data.Net.Security {
	public interface IRequiresProviderContext {
		OAuthProvider Context { get; set; }
	}
}