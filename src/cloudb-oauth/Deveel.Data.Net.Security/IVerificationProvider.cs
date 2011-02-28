using System;

namespace Deveel.Data.Net.Security {
	public interface IVerificationProvider {
		string GenerateVerifier(IRequestToken token);

		bool IsValid(IRequestToken token, string verifier);
	}
}