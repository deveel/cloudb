using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public class PathInfo {
		private readonly string pathName;
		private readonly string pathType;
		private readonly int version;
		private readonly IServiceAddress rootLeader;
		private readonly IServiceAddress[] rootServers;

		internal PathInfo(string pathName, string pathType, int version, IServiceAddress rootLeader, IServiceAddress[] rootServers) {
			this.pathName = pathName;
			this.pathType = pathType;
			this.version = version;
			this.rootLeader = rootLeader;
			this.rootServers = rootServers;

			// Check the root leader in the root servers list,
			bool found = false;
			for (int i = 0; i < rootServers.Length; ++i) {
				if (rootServers[i].Equals(rootLeader)) {
					found = true;
					break;
				}
			}
			// Error if not found,
			if (!found) {
				throw new ApplicationException("Leader not found in root servers list");
			}
		}

		public string PathName {
			get { return pathName; }
		}

		public string PathType {
			get { return pathType; }
		}

		public int Version {
			get {
				if (version < 0)
					throw new ApplicationException("Negative version number");

				return version;
			}
		}

		public IServiceAddress RootLeader {
			get { return rootLeader; }
		}

		public IServiceAddress[] RootServers {
			get { return (IServiceAddress[]) rootServers.Clone(); }
		}

		public override string ToString() {
			StringBuilder b = new StringBuilder();
			b.Append(pathType);
			b.Append(",");
			b.Append(version);
			// Output the root servers list
			int sz = rootServers.Length;
			for (int i = 0; i < sz; ++i) {
				b.Append(",");
				IServiceAddress addr = rootServers[i];
				// The root leader has a "*" prefix
				if (addr.Equals(rootLeader)) {
					b.Append("*");
				}
				b.Append(addr.ToString());
			}
			return b.ToString();
		}

		public static PathInfo Parse(string pathName, string s) {
			string[] parts = s.Split(',');

			try {
				string type = parts[0];
				int version = Int32.Parse(parts[1]);
				int sz = parts.Length - 2;
				IServiceAddress leader = null;
				IServiceAddress[] servers = new IServiceAddress[sz];

				for (int i = 0; i < sz; ++i) {
					bool isLeader = false;
					String item = parts[i + 2];
					if (item.StartsWith("*")) {
						item = item.Substring(1);
						isLeader = true;
					}
					IServiceAddress addr = ServiceAddresses.ParseString(item);
					servers[i] = addr;
					if (isLeader) {
						leader = addr;
					}
				}

				// Return the PathInfo object,
				return new PathInfo(pathName, type, version, leader, servers);
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		private static bool ListsEqual(IServiceAddress[] list1, IServiceAddress[] list2) {
			if (list1 == null && list2 == null)
				return true;
			if (list1 == null || list2 == null)
				return false;

			// Both non-null
			int sz = list1.Length;
			if (sz != list2.Length)
				return false;

			for (int i = 0; i < sz; ++i) {
				IServiceAddress addr = list1[i];
				bool found = false;
				for (int n = 0; n < sz; ++n) {
					if (list2[n].Equals(addr)) {
						found = true;
						break;
					}
				}
				if (!found) {
					return false;
				}
			}
			for (int i = 0; i < sz; ++i) {
				IServiceAddress addr = list2[i];
				bool found = false;
				for (int n = 0; n < sz; ++n) {
					if (list1[n].Equals(addr)) {
						found = true;
						break;
					}
				}
				if (!found) {
					return false;
				}
			}
			return true;
		}

		public override bool Equals(object ob) {
			PathInfo that_path_info = ob as PathInfo;
			if (that_path_info == null)
				return false;
			if (pathName.Equals(that_path_info.pathName) &&
				pathType.Equals(that_path_info.pathType) &&
				version == that_path_info.version &&
				rootLeader.Equals(that_path_info.rootLeader) &&
				ListsEqual(rootServers, that_path_info.rootServers)) {
				return true;
			}
			return false;
		}

		public override int GetHashCode() {
			int servers_hashcode = 0;
			foreach (IServiceAddress addr in rootServers) {
				servers_hashcode += addr.GetHashCode();
			}
			return pathName.GetHashCode() + pathType.GetHashCode() + version + rootLeader.GetHashCode() +
				   servers_hashcode;
		}

	}
}