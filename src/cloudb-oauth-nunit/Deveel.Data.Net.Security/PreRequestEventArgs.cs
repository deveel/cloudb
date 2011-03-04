using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public class PreRequestEventArgs : EventArgs {
		private Uri requestUri;
		private string httpMethod;
		private readonly NameValueCollection parameters;
		private Uri callbackUrl;

		internal PreRequestEventArgs(Uri requestUri, string httpMethod, Uri callbackUrl) {
			this.requestUri = requestUri;
			this.httpMethod = httpMethod;
			parameters = new NameValueCollection();
			this.callbackUrl = callbackUrl;
		}

		public Uri RequestUri {
			get { return requestUri; }
			set { requestUri = value; }
		}

		public string HttpMethod {
			get { return httpMethod; }
			set { httpMethod = value; }
		}

		public Uri CallbackUrl {
			get { return callbackUrl; }
			set { callbackUrl = value; }
		}

		public NameValueCollection AdditionalParameters {
			get { return parameters; }
		}
	}
}