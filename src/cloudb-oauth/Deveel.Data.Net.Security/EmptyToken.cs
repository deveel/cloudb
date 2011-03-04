using System;

namespace Deveel.Data.Net.Security {
	public class EmptyToken : IToken {
		private readonly string consumerKey;
		private readonly TokenType tokenType;

		public EmptyToken(string consumerKey, TokenType tokenType) {
			this.consumerKey = consumerKey;
			this.tokenType = tokenType;
		}

		public string Token {
			get { return null; }
		}

		public string Secret {
			get { return null; }
		}

		public string ConsumerKey {
			get { return consumerKey; }
		}

		public TokenType Type {
			get { return tokenType; }
		}
	}
}