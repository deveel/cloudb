using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security.Fake {
	public sealed class FakeHttpRequest : IHttpRequest {
		private string httpMethod;
		private Uri url;
		private readonly NameValueCollection queryString;
		private readonly NameValueCollection form;
		private readonly NameValueCollection headers;

		public FakeHttpRequest() {
			queryString = new NameValueCollection();
			form = new NameValueCollection();
			headers = new NameValueCollection();
		}

		public string HttpMethod {
			get { return httpMethod; }
			set { httpMethod = value; }
		}

		public Uri Url {
			get { return url; }
			set { url = value; }
		}

		public string RawUrl {
			get { return url.ToString(); }
		}

		public NameValueCollection QueryString {
			get { return queryString; }
		}

		public NameValueCollection Form {
			get { return form; }
		}

		public NameValueCollection Headers {
			get { return headers; }
		}
	}
}