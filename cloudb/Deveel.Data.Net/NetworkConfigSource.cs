using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace Deveel.Data.Net {
	public class NetworkConfigSource : ConfigSource {
		private readonly string source;
		private readonly object stateLock = new object();
		private DateTime lastReadTime;

		private IServiceAddress[] networkNodes;

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
		
		public NetworkConfigSource()
			: this((string)null) {
		}

		public IServiceAddress[] NetworkNodes {
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

		private static IServiceAddress[] ParseAddress(string value) {
			List<IServiceAddress> addresses = new List<IServiceAddress>();
			string[] sp = value.Split(',');
			for (int i = 0; i < sp.Length; i++) {
				string s = sp[i].Trim();
				if (s.Length > 0)
					addresses.Add(ServiceAddresses.ParseString(s));
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
		
		public void AddNetworkNode(IServiceAddress address) {
			lock(stateLock) {
				IServiceAddress[] nodes;
				string value = GetString(NetworkNodeList, null);
				if (value != null) {
					nodes = ParseAddress(value);
					int index = Array.BinarySearch(nodes, address);
					if (index >= 0)
						throw new ArgumentException("The address '" + address +  "' is already present.");
					IServiceAddress[] oldNodes = nodes;
					nodes = new IServiceAddress[oldNodes.Length + 1];
					Array.Copy(oldNodes, 0, nodes, 0, oldNodes.Length);
					nodes[nodes.Length - 1] = address;
				} else {
					nodes = new IServiceAddress[] { address };
				}
				
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < nodes.Length; i++) {
					IServiceAddressHandler handler = ServiceAddresses.GetHandler(nodes[i]);
					sb.Append(handler.ToString(nodes[i]));
					
					if (i < nodes.Length - 1)
						sb.Append(", ");
				}
				
				SetValue(NetworkNodeList, sb.ToString());
			}
		}
		
		public void AddNetworkNode(string address) {
			if (String.IsNullOrEmpty(address))
				throw new ArgumentNullException("address");
			
			IServiceAddress serviceAddress = ServiceAddresses.ParseString(address);
			if (serviceAddress == null)
				throw new ArgumentException("The address '" + address + "' is not supported.");
			
			AddNetworkNode(address);
		}
		
		public void AddAllowedIp(string address) {
			lock(stateLock) {
				string[] addresses;
				string whiteList = GetString(ConnectWhiteList);
				if (!String.IsNullOrEmpty(whiteList)) {
					if (address.Equals("*") &&
					    whiteList.Equals("*"))
						return;
					
					string[] sp = whiteList.Split(',');
					addresses = new string[sp.Length];
					for(int i = 0; i < sp.Length; i++) {
						string s = sp[i].Trim();
						if (s.Equals(address))
							throw new ArgumentException("The address '" + address + "' is already present in the whitelist.");
						
						addresses[i] = s;
					}
					
					string[] oldAddresses = addresses;
					addresses = new string[oldAddresses.Length + 1];
					Array.Copy(oldAddresses, 0, addresses, 0, oldAddresses.Length);
					addresses[addresses.Length - 1] = address;
				} else {
					addresses = new string[] { address };
				}
				
				whiteList = String.Join(", ", addresses);
				SetValue(ConnectWhiteList, whiteList);
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
			if (String.IsNullOrEmpty(source))
				return;
			
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