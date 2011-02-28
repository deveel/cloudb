using System;

namespace Deveel.Data.Net.Security {
	public interface IRequestIdValidator {
		RequestId ValidateRequest(string nonce, long timestamp, string consumerKey, string requestToken);
	}
}