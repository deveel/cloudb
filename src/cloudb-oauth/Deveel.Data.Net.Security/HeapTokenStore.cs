using System;
using System.Collections.Generic;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public sealed class HeapTokenStore : ITokenStore {
		private readonly IDictionary<string, IToken> accessTokens;
		private readonly IDictionary<string, IToken> requestTokens;

		private readonly object SyncRoot = new object();

		public HeapTokenStore() {
			accessTokens = new Dictionary<string, IToken>();
			requestTokens = new Dictionary<string, IToken>();
		}

		public bool Add(IToken token) {
			if (token == null)
				throw new ArgumentNullException("token");

			lock (SyncRoot) {
				if (token.Type == TokenType.Access) {
					if (accessTokens.ContainsKey(token.Token))
						return false;
					accessTokens.Add(token.Token, token);
				} else {
					if (requestTokens.ContainsKey(token.Token))
						return false;
					requestTokens.Add(token.Token, token);
				}

				return true;
			}
		}

		public IToken Get(string token, TokenType type) {
			if (String.IsNullOrEmpty(token))
				throw new ArgumentNullException("token");

			lock (SyncRoot) {
				IToken storedToken;

				if (type == TokenType.Access) {
					accessTokens.TryGetValue(token, out storedToken);
				} else {
					requestTokens.TryGetValue(token, out storedToken);
				}

				return storedToken;
			}
		}

		public IList<IToken> FindByUser(IIdentity user, string consumerKey) {
			if (user == null)
				throw new ArgumentNullException("user");

			lock (SyncRoot) {
				List<IToken> tokens = new List<IToken>();
				foreach (KeyValuePair<string, IToken> pair in accessTokens) {
					IAccessToken token = pair.Value as IAccessToken;
					if (token == null)
						continue;

					if (token.RequestToken.AuthenticatedUser.Equals(user)) {
						if (!String.IsNullOrEmpty(consumerKey) && 
							!token.ConsumerKey.Equals(consumerKey))
							continue;
						tokens.Add(token);
					}
				}

				foreach (KeyValuePair<string, IToken> pair in requestTokens) {
					IRequestToken token = pair.Value as IRequestToken;
					if (token == null)
						continue;

					if (token.AuthenticatedUser.Equals(user)) {
						if (!String.IsNullOrEmpty(consumerKey) &&
							!token.ConsumerKey.Equals(consumerKey))
							continue;

						tokens.Add(token);
					}
				}

				return tokens.AsReadOnly();
			}
		}

		public IList<IToken> FindByConsumer(string consumerKey) {
			if (String.IsNullOrEmpty(consumerKey))
				throw new ArgumentNullException("consumerKey");

			lock (SyncRoot) {
				List<IToken> tokens = new List<IToken>();
				foreach (KeyValuePair<string, IToken> pair in accessTokens) {
					if (pair.Key.Equals(consumerKey))
						tokens.Add(pair.Value);
				}

				foreach (KeyValuePair<string, IToken> pair in requestTokens) {
					if (pair.Key.Equals(consumerKey))
						tokens.Add(pair.Value);
				}

				return tokens.AsReadOnly();
			}
		}

		public bool Update(IToken token) {
			if (token == null)
				throw new ArgumentNullException("token");

			lock (SyncRoot) {
				if (token.Type == TokenType.Request) {
					if (!requestTokens.ContainsKey(token.Token))
						return false;

					requestTokens[token.Token] = token;
				} else {
					if (!accessTokens.ContainsKey(token.Token))
						return false;

					accessTokens[token.Token] = token;
				}
			}

			return true;
		}

		public bool Remove(IToken token) {
			if (token == null)
				throw new ArgumentNullException("token");

			lock (SyncRoot) {
				if (token.Type == TokenType.Access)
					return accessTokens.Remove(token.Token);
				return requestTokens.Remove(token.Token);
			}
		}
	}
}