using System;

namespace Deveel.Data.Net.Security {
	public sealed class RequestState {
		// This class is sealed because implementors of IRequestStateStore cannot
		// be expected to be able to store arbitrary additional data that may be
		// added in derived classes. 

		private readonly RequestStateKey key;
		private IToken requestToken;
		private IToken accessToken;

		public RequestState(RequestStateKey key) {
			if (key == null)
				throw new ArgumentNullException("key");

			this.key = key;
		}

		public RequestStateKey Key {
			get { return key; }
		}

		public IToken RequestToken {
			get { return requestToken; }
			set { requestToken = value; }
		}

		public IToken AccessToken {
			get { return accessToken; }
			set { accessToken = value; }
		}
	}
}