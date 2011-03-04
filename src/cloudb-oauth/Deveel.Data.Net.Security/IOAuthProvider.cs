using System;

namespace Deveel.Data.Net.Security {
	public interface IOAuthProvider {
		IConsumerStore ConsumerStore { get; }

		ICallbackStore CallbackStore { get; }

		ITokenStore TokenStore { get; }

		ITokenGenerator TokenGenerator { get; }

		IRequestIdValidator RequestIdValidator { get; }

		IVerificationProvider VerificationProvider { get; }

		IResourceAccessVerifier ResourceAccessVerifier { get; }
	}
}