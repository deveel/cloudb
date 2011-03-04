using System;

namespace Deveel.Data.Net.Security {
	public class OAuthResponse {
		private readonly IToken token;
		private readonly bool hasResource;
		private readonly OAuthResource resource;

		internal OAuthResponse(IToken token)
			: this(token, null) {
		}

		internal OAuthResponse(IToken token, OAuthResource resource) {
			this.token = token;
			this.resource = resource;
			hasResource = resource != null;
		}

		public IToken Token {
			get { return token; }
		}

		public bool HasProtectedResource {
			get { return hasResource; }
		}

		public OAuthResource ProtectedResource {
			get { return resource; }
		}
	}
}