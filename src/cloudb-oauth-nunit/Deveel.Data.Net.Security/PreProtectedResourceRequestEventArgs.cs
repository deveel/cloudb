using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public class PreProtectedResourceRequestEventArgs : EventArgs {
		private Uri requestUri;
		private string httpMethod;
		private readonly IToken requestToken;
		private readonly IToken accessToken;
		private readonly NameValueCollection parameters;

		public PreProtectedResourceRequestEventArgs(Uri requestUri, string httpMethod, IToken requestToken, IToken accessToken) {
			this.requestUri = requestUri;
			this.httpMethod = httpMethod;
			parameters = new NameValueCollection();
			this.requestToken = requestToken;
			this.accessToken = accessToken;
		}

		public Uri RequestUri {
			get { return requestUri; }
			set { requestUri = value; }
		}

		public string HttpMethod {
			get { return httpMethod; }
			set { httpMethod = value; }
		}

		public NameValueCollection AdditionalParameters {
			get { return parameters; }
		}

		public IToken RequestToken {
			get { return requestToken; }
		}

		public IToken AccessToken {
			get { return accessToken; }
		}
	}

}