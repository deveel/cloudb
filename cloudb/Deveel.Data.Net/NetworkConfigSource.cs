using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Deveel.Data.Net {
	public class NetworkConfigSource : ConfigSource {
		private readonly string source;
		private readonly object stateLock = new object();
		private DateTime lastReadTime;

		private ServiceAddress[] networkNodes;

		public const string ConnectWhiteList = "connect_whitelist";
		public const string NetworkNodeList = "network_nodelist";
		public const string ConfigCheckTimeout = "configcheck_timeout";

		public NetworkConfigSource(Stream stream)
			: base(stream) {
		}

		public NetworkConfigSource(string source) {
			this.source = source;
			Reload();
		}

		public ServiceAddress[] NetworkNodes {
			get {
				lock(stateLock) {
					if (networkNodes == null) {
						string value = GetString(NetworkNodeList, null);
						if (value != null)
							networkNodes = ParseAddress(value);
					}

					return networkNodes;
				}
			}
		}

		public int CheckTimeout {
			get {
				lock(stateLock) {
					return GetInt32(ConfigCheckTimeout);
				}
			}
		}

		public bool AllowAll {
			get {
				lock(stateLock) {
					string value = GetString(ConnectWhiteList, null);
					if (value == null)
						return false;

					bool allowedAll;
					List<string> allowedIp, catchall;
					ParseConnectWhiteList(value, out allowedAll, out allowedIp, out catchall);
					return allowedAll;
				}
			}
		}

		private static ServiceAddress[] ParseAddress(string value) {
			List<ServiceAddress> addresses = new List<ServiceAddress>();
			string[] sp = value.Split(',');
			for (int i = 0; i < sp.Length; i++) {
				string s = sp[i].Trim();
				if (s.Length > 0)
					addresses.Add(ServiceAddress.Parse(s));
			}

			return addresses.ToArray();
		}

		private static void ParseConnectWhiteList(string value, out bool allowAll, out List<string> allowedIp, out List<string> catchAll) {
			value = value.Trim();
			if (value.Equals("*")) {
				allowAll = true;
				allowedIp = null;
				catchAll = null;
			} else {
				allowAll = false;

				allowedIp = new List<string>();
				catchAll = new List<string>();

				string[] sp = value.Split(',');
				for (int i = 0; i < sp.Length; i++) {
					string s = sp[i].Trim();
					if (s.Length > 0) {
						if (s.EndsWith(".*")) {
							catchAll.Add(s.Substring(0, s.Length - 2));
						} else {
							allowedIp.Add(s);
						}
					}
				}
			}
		}

		public bool IsIpAllowed(string address) {
			lock(stateLock) {
				string whiteList = GetString(ConnectWhiteList);
				List<string> allowedIp, catchAll;
				bool allowAll;
				ParseConnectWhiteList(whiteList, out allowAll, out allowedIp, out catchAll);
				if (allowAll)
					return true;

				if (catchAll != null) {
					for (int i = 0; i < catchAll.Count; i++) {
						if (address.StartsWith(catchAll[i]))
							return true;
					}
				}

				if (allowedIp != null) {
					for (int i = 0; i < allowedIp.Count; i++) {
						if (allowedIp[i].Equals(address))
							return true;
					}
				}

				return false;
			}
		}

		internal void Reload() {
			Uri uri;
			if (Uri.TryCreate(source,UriKind.RelativeOrAbsolute, out uri)) {
				if (uri.Scheme == Uri.UriSchemeFile) {
					//TODO: ...
				} else if (uri.Scheme == Uri.UriSchemeHttp || 
					uri.Scheme == Uri.UriSchemeHttps ||
					uri.Scheme == Uri.UriSchemeFtp) {
					WebRequest request = WebRequest.Create(uri);
					WebResponse response = request.GetResponse();

					DateTime modTime;
					if (response is HttpWebResponse) {
						if (((HttpWebResponse)response).StatusCode != HttpStatusCode.OK)
							throw new InvalidOperationException();

						modTime = ((HttpWebResponse) response).LastModified;
					} else if (response is FtpWebResponse) {
						if (((FtpWebResponse)response).StatusCode != FtpStatusCode.CommandOK)
							throw new InvalidOperationException();

						modTime = ((FtpWebResponse) response).LastModified;
					} else {
						throw new NotSupportedException();
					}

					if (lastReadTime != DateTime.MinValue && modTime > lastReadTime) {
						using(Stream inputStream = response.GetResponseStream()) {
							Load(inputStream);
						}

						lastReadTime = modTime;
					}
				}
			} else if (File.Exists(source)) {
				DateTime modTime = File.GetLastWriteTime(source);
				if (lastReadTime != DateTime.MinValue && modTime > lastReadTime) {
					using(FileStream fileStream = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read)) {
						Load(fileStream);
					}

					lastReadTime = modTime;
				}
			} else {
				throw new InvalidOperationException("The source type is unknown.");
			}
		}
	}
}