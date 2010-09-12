using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class MemoryRootService : RootService {
		private readonly Dictionary<string, string> pathTypes = new Dictionary<string, string>(128);
		private readonly Dictionary<string, PathAccess> pathAccessMap = new Dictionary<string, PathAccess>(128);
		private readonly List<string> deletedPaths = new List<string>(128);

		public MemoryRootService(IServiceConnector connector) 
			: base(connector) {
		}

		protected override PathAccess FetchPathAccess(string pathName) {
			PathAccess access;
			if (!pathAccessMap.TryGetValue(pathName, out access)) {
				string pathTypeName;
				if (!pathTypes.TryGetValue(pathName, out pathTypeName))
					throw new ApplicationException("Path '" + pathName + "' not found input this root service.");

				if (deletedPaths.Contains(pathName))
					throw new ApplicationException("Path '" + pathName + "' did exist but was deleted.");

				access = new PathAccess(new MemoryStream(), pathName, pathTypeName);
				pathAccessMap[pathName] = access;
			}

			return access;
		}

		protected override void CreatePath(string pathName, string pathTypeName) {
			if (pathTypes.ContainsKey(pathName))
				throw new ApplicationException("Path '" + pathName + "' exists on this root service.");

			pathTypes[pathName] = pathTypeName;
		}

		protected override void DeletePath(string pathName) {
			if (!pathTypes.ContainsKey(pathName))
				throw new ApplicationException("Path '" + pathName + "' doesn't exist on this root service.");

			deletedPaths.Add(pathName);
		}

		protected override IList<PathStatus> ListPaths() {
			List<PathStatus> list = new List<PathStatus>();
			foreach(KeyValuePair<string, string> pair in pathTypes) {
				bool deleted = deletedPaths.Contains(pair.Key);
				list.Add(new PathStatus(pair.Key, deleted));
			}
			return list;
		}
	}
}