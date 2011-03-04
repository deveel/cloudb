using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public class AccessTokenReceivedEventArgs : EventArgs {
		private readonly IToken requestToken;
		private readonly IToken accessToken;
		private readonly NameValueCollection parameters;

		public AccessTokenReceivedEventArgs(IToken requestToken, IToken accessToken) {
			this.requestToken = requestToken;
			this.accessToken = accessToken;
			parameters = new NameValueCollection();
		}

		public IToken RequestToken {
			get { return requestToken; }
		}

		public IToken AccessToken {
			get { return accessToken; }
		}

		public NameValueCollection AdditionalParameters {
			get { return parameters; }
		}
	}
}