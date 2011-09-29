using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	/// <summary>
	/// Contains all the information necessary for a request for
	/// authentication between services or client and service
	/// </summary>
	/// <remarks>
	/// A request for authentication between two services is a
	/// rare condition, that happens when services within the same
	/// network enforce the security by establishing internal
	/// authentication mechanisms.
	/// <para>
	/// In a request between services the name of the path is omitted.
	/// </para>
	/// <para>
	/// The most common use case of a request for authentication
	/// is at the beginning of a communication session between
	/// a client and the administration service of network.
	/// </para>
	/// </remarks>
	public sealed class AuthRequest {
		private readonly string pathName;
		private readonly object context;
		private readonly IDictionary<string, AuthObject> authData;

		public AuthRequest(object context, string pathName) {
			this.pathName = pathName;
			this.context = context;
			authData = new Dictionary<string, AuthObject>(32);
		}

		public AuthRequest(object context, string pathName, IDictionary<string, AuthObject> authData)
			: this(context, pathName) {
			if (authData == null)
				return;

			foreach (KeyValuePair<string, AuthObject> pair in authData)
				this.authData[pair.Key] = pair.Value;
		}

		public AuthRequest(object context)
			: this(context, (string) null) {
		}

		public AuthRequest(object context, IDictionary<string, AuthObject> authData)
			: this(context, null, authData) {
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

		public bool HasPathName {
			get { return String.IsNullOrEmpty(pathName); }
		}
	}
}