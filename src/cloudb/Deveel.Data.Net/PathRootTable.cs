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