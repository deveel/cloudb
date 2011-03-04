using System;

namespace Deveel.Data.Net.Security {
	public sealed class GuidNonceGenerator : INonceGenerator {
		public string GenerateNonce(int timestamp) {
			return Guid.NewGuid().ToString("D");
		}
	}
}