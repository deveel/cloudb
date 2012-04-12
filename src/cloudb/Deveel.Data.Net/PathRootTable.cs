using System;

namespace Deveel.Data.Net {
	class PathRootTable {
		public PathRootTable(IDataFile data) {
			dictionary = new StringDictionary(data);
		}

		private readonly StringDictionary dictionary;

		public ISortedCollection<string> Keys {
			get { return dictionary.Keys; }
		}

		public void Set(String path, IServiceAddress rootAddress) {
			string rootAddrStr = null;
			if (rootAddress != null)
				rootAddrStr = rootAddress.ToString();

			dictionary.SetValue(path, rootAddrStr);
		}

		public IServiceAddress Get(string path) {
			string rootServerStr = dictionary.GetValue(path);
			if (rootServerStr == null)
				return null;

			try {
				return ServiceAddresses.ParseString(rootServerStr);
			} catch (Exception e) {
				throw new FormatException("Unable to parse service address: " + e.Message);
			}
		}
	}
}