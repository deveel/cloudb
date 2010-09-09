using System;

using Deveel.Data.Store;

namespace Deveel.Data.Net {
	public sealed class PathRootTable {
		public PathRootTable(DataFile data) {
			properties = new Properties(data);
		}

		private readonly Properties properties;

		public ISortedCollection<string> Keys {
			get { return properties.Keys; }
		}

		public void Set(String path, ServiceAddress rootAddress) {
			string rootAddrStr = null;
			if (rootAddress != null)
				rootAddrStr = rootAddress.ToString();

			properties.SetProperty(path, rootAddrStr);
		}

		public ServiceAddress Get(string path) {
			string rootServerStr = properties.GetProperty(path);
			if (rootServerStr == null)
				return null;

			try {
				return ServiceAddress.Parse(rootServerStr);
			} catch (Exception e) {
				throw new FormatException("Unable to parse service address: " + e.Message);
			}
		}
	}
}