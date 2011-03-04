using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

using Deveel.Data.Configuration;
using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	public static class SignProviders {
		public static readonly ISignProvider HmacSha1 = new HmacSha1SignProvider();
		public static readonly ISignProvider PlainText = new PlainTextSignProvider();
		public static readonly ISignProvider RsaSha1 = new RsaSha1SignProvider();

		public static ISignProvider GetProvider(string method) {
			if (String.Compare(method, "HMAC-SHA1", true) == 0)
				return HmacSha1;
			if (String.Compare(method, "PLAINTEXT", true) == 0)
				return PlainText;
			if (String.Compare(method, "RSA-SHA1", true) == 0)
				return RsaSha1;

			throw new NotSupportedException("Sign method '" + method + "' not currently supported.");
		}

		#region HmacSha1SignProvider

		private class HmacSha1SignProvider : ISignProvider {
			public string SignatureMethod {
				get { return "HMAC-SHA1"; }
			}

			public void Configure(ConfigSource config) {
			}

			public string ComputeSignature(string signatureBase, string consumerSecret, string tokenSecret) {
				using (HMACSHA1 crypto = new HMACSHA1()) {
					string key = Rfc3986.Encode(consumerSecret) + "&" + Rfc3986.Encode(tokenSecret);
					crypto.Key = Encoding.ASCII.GetBytes(key);
					string hash = Convert.ToBase64String(crypto.ComputeHash(Encoding.ASCII.GetBytes(signatureBase)));
					crypto.Clear();
					return hash;
				}
			}

			public bool ValidateSignature(string signatureBase, string signature, string consumerSecret, string tokenSecret) {
				string expectedSignature = ComputeSignature(signatureBase, consumerSecret, tokenSecret);
				string actualSignature = Rfc3986.Decode(signature);
				return expectedSignature == actualSignature;
			}
		}

		#endregion

		#region PlainTextSignProvider

		private class PlainTextSignProvider : ISignProvider {
			public string SignatureMethod {
				get { return "PLAINTEXT"; }
			}

			public void Configure(ConfigSource config) {
			}

			public string ComputeSignature(string signatureBase, string consumerSecret, string tokenSecret) {
				StringBuilder signature = new StringBuilder();

				if (!String.IsNullOrEmpty(consumerSecret))
					signature.Append(Rfc3986.Encode(consumerSecret));

				signature.Append("&");

				if (!String.IsNullOrEmpty(tokenSecret))
					signature.Append(Rfc3986.Encode(tokenSecret));

				return signature.ToString();
			}

			public bool ValidateSignature(string signatureBase, string signature, string consumerSecret, string tokenSecret) {
				string expectedSignature = ComputeSignature(signatureBase, consumerSecret, tokenSecret);

				return expectedSignature == signature;
			}
		}

		#endregion

		#region RsaSha1SigningProvider

		private class RsaSha1SignProvider : ISignProvider {
			private string pfxFileName;
			private string pfxFilePassword;
			private X509Certificate2 cert;

			public string SignatureMethod {
				get { return "RSA-SHA1"; }
			}

			private X509Certificate2 Certificate {
				get {
					if (cert == null)
						cert = new X509Certificate2(pfxFileName, pfxFilePassword);

					return cert;
				}
			}

			public void Configure(ConfigSource config) {
				pfxFileName = config.GetString("pfxFile");
				if (String.IsNullOrEmpty(pfxFileName))
					throw new ConfigurationException(config, "pfxFile");

				pfxFilePassword = config.GetString("pfxPassword");
			}

			public string ComputeSignature(string signatureBase, string consumerSecret, string tokenSecret) {
				if (Certificate == null || Certificate.PrivateKey == null)
					throw new InvalidOperationException("Required X509 Certificate containing a private key was not found.");

				using (HashAlgorithm hasher = HashAlgorithm.Create("SHA1")) {
					RSAPKCS1SignatureFormatter signatureFormatter = new RSAPKCS1SignatureFormatter(Certificate.PrivateKey);
					signatureFormatter.SetHashAlgorithm("SHA1");

					byte[] sigBaseBytes = Encoding.ASCII.GetBytes(signatureBase);
					byte[] hash = hasher.ComputeHash(sigBaseBytes);

					return Convert.ToBase64String(signatureFormatter.CreateSignature(hash));
				}
			}

			public bool ValidateSignature(string signatureBase, string signature, string consumerSecret, string tokenSecret) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}