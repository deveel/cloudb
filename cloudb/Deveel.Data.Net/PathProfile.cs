using System;

namespace Deveel.Data.Net {
	public class PathProfile {
		internal PathProfile(IServiceAddress rootAddress, string path, string pathType) {
			this.rootAddress = rootAddress;
			this.path = path;
			this.pathType = pathType;
		}

		private readonly IServiceAddress rootAddress;
		private readonly String path;
		private readonly String pathType;

		public string Path {
			get { return path; }
		}

		public IServiceAddress RootAddress {
			get { return rootAddress; }
		}

		public string PathType {
			get { return pathType; }
		}
	}
}