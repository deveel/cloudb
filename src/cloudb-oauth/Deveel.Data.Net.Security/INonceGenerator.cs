using System;

namespace Deveel.Data.Net.Security {
	public interface INonceGenerator {
		string GenerateNonce(int timestamp);
	}
}