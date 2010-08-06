using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Deveel.Data.Net {
	public class NetworkConfiguration {
		protected NetworkConfiguration(string sourceString, NetworkConfigurationSource sourceType) {
			this.sourceString = sourceString;
			this.sourceType = sourceType;
		}

		private DateTime last_read;
		private bool allow_all_ips = false;
		private List<String> allowed_ips = new List<string>();
		private List<String> catchall_allowed_ips = new List<string>();
		private String all_machine_nodes = "";
		private int configcheck_timeout;

		private NetworkConfigurationSource sourceType;
		private string sourceString;

		private readonly Object state_lock = new Object();

		public string NetworkNodelist {
			get {
				lock(state_lock) {
					return all_machine_nodes;
				}
			}
		}

		public int CheckTimeout {
			get {
				lock(state_lock) {
					return configcheck_timeout;
				}
			}
		}

		public virtual DateTime LastModified {
			get { return last_read; }
		}

		private void Load(Stream input, DateTime lastModified) {
			if (lastModified == DateTime.MinValue || lastModified != last_read) {
				last_read = lastModified;

				Util.Properties p = new Util.Properties();
				p.Load(input);

				String connect_whitelist = p.GetProperty("connect_whitelist");
				String network_nodelist = p.GetProperty("network_nodelist");

				if (connect_whitelist == null || connect_whitelist.Equals("")) {
					throw new IOException("Unable to find 'connect_whitelist' property in " +
										  "the network configuration resource.");
				}
				if (network_nodelist == null || network_nodelist.Equals("")) {
					throw new IOException("Unable to find 'network_nodelist' property in " +
										  "the network configuration resource.");
				}

				int set_conf_timeout = 2 * 60;
				String conf_timeout = p.GetProperty("configcheck_timeout");
				if (conf_timeout != null) {
					conf_timeout = conf_timeout.Trim();
					try {
						set_conf_timeout = Int32.Parse(conf_timeout);
					} catch (Exception e) {
						//TODO: WARNING log ...
					}
				}

				//TODO: INFO log...

				List<String> all_ips = new List<string>();
				List<String> call_allowed_ips = new List<string>();
				bool alla_ips = false;

				// Is it catchall whitelist?
				if (connect_whitelist.Trim().Equals("*")) {
					alla_ips = true;
				} else {
					string[] whitelist_ips = connect_whitelist.Split(',');
					foreach (String ip in whitelist_ips) {
						string ip1 = ip.Trim();
						// Is it a catch all ip address?
						if (ip1.EndsWith(".*")) {
							// Add to the catchall list,
							call_allowed_ips.Add(ip1.Substring(0, ip1.Length - 2));
						} else {
							// Add to the ip hashset
							all_ips.Add(ip);
						}
					}
				}

				// Synchronize on 'state_lock' while we update the state,
				lock (state_lock) {
					// The list of all machine nodes,
					all_machine_nodes = network_nodelist;
					allow_all_ips = alla_ips;
					catchall_allowed_ips = call_allowed_ips;
					allowed_ips = all_ips;
					configcheck_timeout = set_conf_timeout;
				}
			}
		}

		public bool IsIpAllowed(String ip_address) {
			lock (state_lock) {
				// The catchall,
				if (allow_all_ips)
					return true;

				// Check the map,
				if (allowed_ips.Contains(ip_address))
					return true;

				// Check the catch all list,
				foreach (string expr in catchall_allowed_ips) {
					if (ip_address.StartsWith(expr))
						return true;
				}

				// No matches,
				return false;
			}
		}

		public virtual void Reload() {
			DateTime lastModified = LastModified;
			Stream input = null;

			if (sourceType == NetworkConfigurationSource.Http ||
				sourceType == NetworkConfigurationSource.Https) {
				WebRequest request = WebRequest.Create(sourceString);
				WebResponse response = request.GetResponse();
				
				// Get the last modified time,
				lastModified = ((HttpWebResponse)response).LastModified;
				input = response.GetResponseStream();
			} else if (sourceType == NetworkConfigurationSource.Ftp) {
				WebRequest request = WebRequest.Create(sourceString);
				WebResponse response = request.GetResponse();

				// Get the last modified time,
				lastModified = ((FtpWebResponse)response).LastModified;
				input = response.GetResponseStream();
			} else if (sourceType == NetworkConfigurationSource.File) {
				FileInfo fileInfo = new FileInfo(sourceString);
				lastModified = fileInfo.LastWriteTime;

				input = fileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
			}

			Load(input, lastModified);
		}

		public static NetworkConfiguration Create(Uri url) {
			NetworkConfigurationSource sourceType;
			if (url.Scheme == Uri.UriSchemeHttp)
				sourceType = NetworkConfigurationSource.Http;
			else if (url.Scheme == Uri.UriSchemeHttps)
				sourceType = NetworkConfigurationSource.Https;
			else
				throw new ArgumentException();

			NetworkConfiguration config = new NetworkConfiguration(url.ToString(), sourceType);
			config.Reload();
			return config;
		}

		public static NetworkConfiguration Create(string file) {
			NetworkConfiguration config = new NetworkConfiguration(file, NetworkConfigurationSource.File);
			config.Reload();
			return config;
		}
	}
}