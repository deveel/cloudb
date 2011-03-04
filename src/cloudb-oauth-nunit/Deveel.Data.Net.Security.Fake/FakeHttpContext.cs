using System;
using System.Security.Principal;

namespace Deveel.Data.Net.Security.Fake {
	public class FakeHttpContext : IHttpContext {
		private FakeHttpRequest request;
		private FakeHttpResponse response;
		private IPrincipal user;

		public FakeHttpRequest Request {
			get { return request; }
			set { request = value; }
		}

		IHttpRequest IHttpContext.Request {
			get { return Request; }
		}

		public FakeHttpResponse Response {
			get { return response; }
			set { response = value; }
		}

		IHttpResponse IHttpContext.Response {
			get { return response; }
		}

		public IPrincipal User {
			get { return user; }
			set { user = value; }
		}
	}
}