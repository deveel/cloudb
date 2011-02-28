using System;

namespace Deveel.Data.Net.Security {
	public interface IToken {
		string Token { get; }

		string Secret { get; }

		string ConsumerKey { get; }

		TokenType Type { get; }
	}
}