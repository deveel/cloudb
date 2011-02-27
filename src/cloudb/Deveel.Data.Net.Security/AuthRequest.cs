using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthRequest {
		private readonly string pathName;
		private readonly object context;
		private readonly IDictionary<string, object> authData;

		public AuthRequest(object context, string pathName) {
			this.pathName = pathName;
			this.context = context;
			authData = new Dictionary<string, object>(32);
		}

		public object Context {
			get { return context; }
		}

		public IDictionary<string, object> AuthData {
			get { return authData; }
		}

		public string PathName {
			get { return pathName; }
		}
	}
}