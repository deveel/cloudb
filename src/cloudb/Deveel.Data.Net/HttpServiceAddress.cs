using System;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class HttpServiceAddress : IServiceAddress {
		private readonly string host;
		private readonly int port;
		private readonly string path;
		private readonly string query;

		private string cachedString;

		public HttpServiceAddress(string host, int port, string path, string query) {
			if (host == null)
				throw new ArgumentNullException("host");

			this.host = host;
			this.query = query;
			this.path = path;
			this.port = port;
		}

		public HttpServiceAddress(string host, int port, string path)
			: this(host, port, path, null) {
		}

		public HttpServiceAddress(string  host, int port)
			: this(host, port, null) {
		}

		public HttpServiceAddress(string host, string path, string query)
			: this(host, -1, path, query) {
		}

		public HttpServiceAddress(string host, string path)
			: this(host, path, null) {
		}

		public HttpServiceAddress(string host)
			: this(host, null) {
		}

		public HttpServiceAddress(Uri uri)
			: this(uri.Host, uri.Port, uri.LocalPath != "/" ? uri.LocalPath : null, uri.Query) {
		}

		public string Host {
			get { return host; }
		}

		public int Port {
			get { return port; }
		}

		public string Path {
			get { return path; }
		}

		public string Query {
			get { return query; }
		}

		int IComparable<IServiceAddress>.CompareTo(IServiceAddress other) {
			if (!(other is HttpServiceAddress))
				throw new ArgumentException();

			return CompareTo((HttpServiceAddress) other);
		}

		public override bool Equals(object obj) {
			HttpServiceAddress other = obj as HttpServiceAddress;
			if (other == null)
				return false;

			if (!host.Equals(other.Host))
				return false;
			if (port >= 0 && port != other.Port)
				return false;

			if (!String.IsNullOrEmpty(path) &&
				!path.Equals(other.Path))
				return false;

			if (!String.IsNullOrEmpty(query) &&
				!query.Equals(other.Query))
				return false;

			return true;
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public int CompareTo(HttpServiceAddress address) {
			if (address == null)
				return -1;

			int c = host.CompareTo(address.host);
			if (c != 0)
				return c;

			c = port.CompareTo(address.port);
			if (c != 0)
				return c;

			if (path == null && address.path == null)
				return 0;
			if (path == null)
				return 1;

			c = path.CompareTo(address.path);
			if (c != 0)
				return c;

			if (query == null && address.query == null)
				return 0;
			if (query == null)
				return 1;

			return query.CompareTo(address.query);
		}

		public Uri ToUri() {
			return new Uri(ToString());
		}

		public override string ToString() {
			if (cachedString == null) {
				UriBuilder builder = new UriBuilder();
				//TODO: allow also HTTPS
				builder.Scheme = Uri.UriSchemeHttp;
				builder.Host = host;
				if (port > 0)
					builder.Port = port;
				builder.Path = path;
				builder.Query = query;
				cachedString = builder.ToString();
			StringBuilder sb = new StringBuilder();
			sb.Append("http://");
			sb.Append(host);
			sb.Append(":");
			sb.Append(port);
			sb.Append("/");
			if (!String.IsNullOrEmpty(path) &&
                !path.Equals("/")) {
				sb.Append(path);
				if (!String.IsNullOrEmpty(query))
					sb.Append("/");
			}

			return cachedString;
		}

		public static HttpServiceAddress Parse(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");
						
			Uri uri = new Uri(s);
			if (uri.Scheme != Uri.UriSchemeHttp)
				throw new FormatException("Invalid scheme: " + uri.Scheme);
			
			return new HttpServiceAddress(uri);
		}
	}
}