using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthRequest {
		private readonly string pathName;
		private readonly object context;
		private readonly IDictionary<string, AuthObject> authData;

		public AuthRequest(object context, string pathName) {
			this.pathName = pathName;
			this.context = context;
			authData = new Dictionary<string, AuthObject>(32);
		}

		public object Context {
			get { return context; }
		}

		public IDictionary<string, AuthObject> AuthData {
			get { return authData; }
		}

		public string PathName {
			get { return pathName; }
		}
	}
}