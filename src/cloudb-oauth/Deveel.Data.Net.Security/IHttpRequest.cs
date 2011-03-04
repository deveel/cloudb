using System;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public interface IHttpRequest {
		string HttpMethod { get; }

		Uri Url { get; }

		string RawUrl { get; }

		NameValueCollection QueryString { get; }

		NameValueCollection Form { get; }

		NameValueCollection Headers { get; }
	}
}