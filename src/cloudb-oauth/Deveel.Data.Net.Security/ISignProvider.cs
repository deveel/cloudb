using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface ISignProvider : IConfigurable {
		string SignatureMethod { get; }


		string ComputeSignature(string signatureBase, string consumerSecret, string tokenSecret);

		bool ValidateSignature(string signatureBase, string signature, string consumerSecret, string tokenSecret);
	}
}