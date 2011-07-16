using System;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class HttpServiceAddress : IServiceAddress {
		private readonly string host;
		private readonly int port;
		private readonly string path;
		private readonly string query;

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
			if (!String.IsNullOrEmpty(query)) {
				sb.Append("?");
				sb.Append(query);
			}

			return sb.ToString();
		}

		public static HttpServiceAddress Parse(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");
			
			/*

			int index = s.IndexOf(':');
			if (index == -1)
				throw new FormatException("Unable to determine the scheme.");

			string scheme = s.Substring(0, index);
			if (scheme != "http")
				throw new FormatException("Scheme not supported.");

			if (s.Length <= index + 3)
				throw new FormatException("The string is too short.");

			s = s.Substring(index + 3);

			string host, path = null, query = null;
			int port = -1;

			host = s;

			index = host.IndexOf('/');
			if (index != -1) {
				host = host.Substring(0, index);
				path = host.Substring(index + 1);

				index = path.IndexOf('?');
				if (index!= -1) {
					query = path.Substring(index + 1);
					path = path.Substring(0, 1);
				}
			}


			index = host.IndexOf(':');
			if (index != -1) {
				host = host.Substring(0, index);
				if (!Int32.TryParse(host.Substring(index + 1), out  port))
					throw new FormatException("Invalid port number.");
			}
			

			return new HttpServiceAddress(host, port, path, query);
			*/
			
			Uri uri = new Uri(s);
			if (uri.Scheme != Uri.UriSchemeHttp)
				throw new FormatException("Invalid scheme: " + uri.Scheme);
			
			return new HttpServiceAddress(uri);
		}
	}
}