using System;

namespace Deveel.Data.Net.Security {
	public interface ITokenGenerator {
		IRequestToken CreateRequestToken(IConsumer consumer, OAuthParameters parameters);

		IAccessToken CreateAccessToken(IConsumer consumer, IRequestToken requestToken);
	}
}