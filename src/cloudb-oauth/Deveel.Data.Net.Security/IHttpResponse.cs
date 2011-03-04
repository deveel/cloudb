using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Security {
	public interface IHttpResponse {
		Encoding ContentEncoding { get; set; }

		string ContentType { get; set; }

		int StatusCode { get; set; }

		Stream OutputStream { get; }

		NameValueCollection Headers { get; }


		void Close();
	}
}