using System;
using System.Security.Cryptography;
using System.Text;

namespace Deveel.Data.Net.Security {
	public class MD5HashVerificationProvider : IVerificationProvider, IRequiresProviderContext {
		private IOAuthProvider context;

		public IOAuthProvider Context {
			get { return context; }
			set { context = value; }
		}

		public string GenerateVerifier(IRequestToken token) {
			return CreateBase64MD5Hash(BuildHashValue(token));
		}

		public bool IsValid(IRequestToken token, string verifier) {
			string hash = CreateBase64MD5Hash(BuildHashValue(token));
			return hash.Equals(verifier, StringComparison.Ordinal);
		}

		private static string CreateBase64MD5Hash(string valueToHash) {
			MD5 md5Provider = new MD5CryptoServiceProvider();
			return Convert.ToBase64String(md5Provider.ComputeHash(Encoding.Unicode.GetBytes(valueToHash)));
		}

		private string BuildHashValue(IRequestToken token) {
			return token.Token + GetConsumerSecret(token.ConsumerKey);
		}

		private string GetConsumerSecret(string consumerKey) {
			IConsumer consumer = context.ConsumerStore.Get(consumerKey);
			if (consumer == null)
				throw new ArgumentException("Consumer could not be found for key " + consumerKey, "consumerKey");

			return consumer.Secret;
		}
	}
}