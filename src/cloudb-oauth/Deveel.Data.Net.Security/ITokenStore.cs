using System;
using System.Collections.Generic;
using System.Security.Principal;

namespace Deveel.Data.Net.Security {
	public interface ITokenStore {
		bool Add(IToken token);

		IToken Get(string token, TokenType type);

		IList<IToken> FindByUser(IIdentity user, string consumerKey);

		IList<IToken> FindByConsumer(string consumerKey);

		bool Update(IToken token);

		bool Remove(IToken token);
	}
}