using System;

namespace Deveel.Data.Net.Security {
	public class PreAccessTokenRequestEventArgs : EventArgs {
		private Uri requestUri;
		private string httpMethod;
		private readonly IToken requestToken;
		private string verifier;

		public PreAccessTokenRequestEventArgs(Uri requestUri, string httpMethod, IToken requestToken, string verifier) {
			this.requestUri = requestUri;
			this.httpMethod = httpMethod;
			this.requestToken = requestToken;
			this.verifier = verifier;
		}

		public Uri RequestUri {
			get { return requestUri; }
			set { requestUri = value; }
		}

		public string HttpMethod {
			get { return httpMethod; }
			set { httpMethod = value; }
		}

		public IToken RequestToken {
			get { return requestToken; }
		}

		public string Verifier {
			get { return verifier; }
			set { verifier = value; }
		}
	}

}