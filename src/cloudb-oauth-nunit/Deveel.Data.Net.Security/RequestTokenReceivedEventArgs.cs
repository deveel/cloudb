using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public class RequestTokenReceivedEventArgs : EventArgs {
		private readonly IToken requestToken;
		private readonly NameValueCollection parameters;

		public RequestTokenReceivedEventArgs(IToken requestToken) {
			this.requestToken = requestToken;
			parameters = new NameValueCollection();
		}

		public IToken RequestToken {
			get { return requestToken; }
		}

		public NameValueCollection Parameters {
			get { return parameters; }
		}
	}
}