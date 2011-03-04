using System;

namespace Deveel.Data.Net.Security {
	public class AuthorizationEventArgs : EventArgs {
		private readonly IToken requestToken;
		private bool continueOnReturn;

		public AuthorizationEventArgs(IToken requestToken) {
			this.requestToken = requestToken;
			continueOnReturn = false;
		}

		public IToken RequestToken {
			get { return requestToken; }
		}

		public bool ContinueOnReturn {
			get { return continueOnReturn; }
			set { continueOnReturn = value; }
		}
	}

}