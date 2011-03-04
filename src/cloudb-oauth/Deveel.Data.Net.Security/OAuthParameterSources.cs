using System;

namespace Deveel.Data.Net.Security {
	[Flags]
	public enum OAuthParameterSources {
		None = 0,
		AuthorizationHeader = 1,
		PostBody = 2,
		QueryString = 4,
		AuthenticateHeader = 8,

		ServiceProviderDefault = AuthorizationHeader | PostBody | QueryString,
		ConsumerDefault = AuthenticateHeader | PostBody
	}
}