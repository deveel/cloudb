using System;

namespace Deveel.Data.Net {
	public class PathProfile {
		internal PathProfile(ServiceAddress rootAddress, string path, string coordination) {
			this.rootAddress = rootAddress;
			this.path = path;
			this.coordination = coordination;
		}

		private readonly ServiceAddress rootAddress;
		private readonly String path;
		private readonly String coordination;

		public string Path {
			get { return path; }
		}

		public ServiceAddress RootAddress {
			get { return rootAddress; }
		}

		public string Coordination {
			get { return coordination; }
		}
	}
}