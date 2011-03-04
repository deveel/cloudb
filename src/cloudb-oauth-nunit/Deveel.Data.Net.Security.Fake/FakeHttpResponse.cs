using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Security.Fake {
	public sealed class FakeHttpResponse : IHttpResponse {
		private Encoding contentEncoding;
		private string contentType;
		private int statusCode;
		private readonly MemoryStream outputStream;
		private readonly NameValueCollection headers;

		public FakeHttpResponse() {
			headers = new NameValueCollection();
			outputStream = new MemoryStream(512);
		}

		public Encoding ContentEncoding {
			get { return contentEncoding; }
			set { contentEncoding = value; }
		}

		public string ContentType {
			get { return contentType; }
			set { contentType = value; }
		}

		public int StatusCode {
			get { return statusCode; }
			set { statusCode = value; }
		}

		public Stream OutputStream {
			get { return outputStream; }
		}

		public NameValueCollection Headers {
			get { return headers; }
		}

		public void Close() {
		}
	}
}