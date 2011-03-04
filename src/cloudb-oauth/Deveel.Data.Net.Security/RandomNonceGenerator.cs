using System;
using System.Security.Cryptography;
using System.Text;

namespace Deveel.Data.Net.Security {
	public sealed class RandomNonceGenerator : INonceGenerator {
		private readonly RNGCryptoServiceProvider random;

		public RandomNonceGenerator() {
			random = new RNGCryptoServiceProvider();
		}

		public string GenerateNonce(int timestamp) {
			MD5 md5 = new MD5CryptoServiceProvider();
			byte[] data = new byte[8];
			random.GetNonZeroBytes(data);
			return Encoding.ASCII.GetString(md5.ComputeHash(data));
		}
	}
}