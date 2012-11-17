//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class PathInfo {
		private readonly string pathName;
		private readonly string pathType;
		private readonly int versionNumber;
		private readonly IServiceAddress rootLeader;
		private readonly IServiceAddress[] rootServers;

		public PathInfo(string pathName, string pathType, int versionNumber, IServiceAddress rootLeader,
		                  IServiceAddress[] rootServers) {
			this.pathName = pathName;
			this.pathType = pathType;
			this.versionNumber = versionNumber;
			this.rootLeader = rootLeader;
			this.rootServers = rootServers;

			// Check the root leader in the root servers list,
			bool found = false;
			foreach (var server in rootServers) {
				if (server.Equals(rootLeader)) {
					found = true;
					break;
				}
			}

			// Error if not found,
			if (!found) {
				throw new ApplicationException("Leader not found in root servers list");
			}
		}

		public IServiceAddress[] RootServers {
			get { return rootServers; }
		}

		public IServiceAddress RootLeader {
			get { return rootLeader; }
		}

		public int VersionNumber {
			get { return versionNumber; }
		}

		public string PathType {
			get { return pathType; }
		}

		public string PathName {
			get { return pathName; }
		}

		public override string ToString() {
			StringBuilder b = new StringBuilder();
			b.Append(pathType);
			b.Append("|");
			b.Append(versionNumber);
			b.Append("|");
			// Output the root servers list
			int sz = rootServers.Length;
			for (int i = 0; i < sz; ++i) {
				IServiceAddress addr = rootServers[i];
				// The root leader has a "*" prefix
				if (addr.Equals(rootLeader)) {
					b.Append("*");
				}
				b.Append(addr);

				if (i < rootServers.Length - 1)
					b.Append("|");
			}
			return b.ToString();
		}

		public static PathInfo Parse(string name, string s) {
			String[] parts = s.Split('|');

			try {
				string type = parts[0];
				int versionNumber = Int32.Parse(parts[1]);
				int sz = parts.Length - 2;
				IServiceAddress rootLeader = null;
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
						rootLeader = addr;
					}
				}

				// Return the PathInfo object,
				return new PathInfo(name, type, versionNumber, rootLeader, servers);
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		private static bool ListsEqual(IServiceAddress[] list1, IServiceAddress[] list2) {

			if (list1 == null && list2 == null) {
				return true;
			}
			if (list1 == null || list2 == null) {
				return false;
			}
			// Both non-null
			int sz = list1.Length;
			if (sz != list2.Length) {
				return false;
			}
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

		public override bool Equals(object obj) {
			PathInfo other = obj as PathInfo;
			if (other == null)
				return false;

			return pathName.Equals(other.pathName) &&
			       pathType.Equals(other.pathType) &&
			       versionNumber == other.versionNumber &&
			       rootLeader.Equals(other.rootLeader) &&
			       ListsEqual(rootServers, other.rootServers);
		}

		public override int GetHashCode() {
			int serversHashcode = 0;
			foreach (IServiceAddress addr in rootServers) {
				serversHashcode += addr.GetHashCode();
			}
			return pathName.GetHashCode() + pathType.GetHashCode() + versionNumber + rootLeader.GetHashCode() + serversHashcode;
		}
	}
}