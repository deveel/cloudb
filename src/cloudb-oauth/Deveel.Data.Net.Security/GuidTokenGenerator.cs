using System;

namespace Deveel.Data.Net.Security {
	public sealed class GuidTokenGenerator : ITokenGenerator {
		public IRequestToken CreateRequestToken(IConsumer consumer, OAuthParameters parameters) {
			return new OAuthRequestToken(Guid.NewGuid().ToString("D"), Guid.NewGuid().ToString("D"), consumer,
			                             TokenStatus.Unauthorized, parameters, null, new string[] {});
		}

		public IAccessToken CreateAccessToken(IConsumer consumer, IRequestToken requestToken) {
			return new OAuthAccessToken(Guid.NewGuid().ToString("D"), Guid.NewGuid().ToString("D"), consumer,
			                            TokenStatus.Unauthorized, requestToken);
		}
	}
}