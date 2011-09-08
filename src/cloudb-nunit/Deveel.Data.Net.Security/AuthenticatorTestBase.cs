using System;

using NUnit.Framework;

namespace Deveel.Data.Net.Security {
	[TestFixture]
	public abstract class AuthenticatorTestBase : NetworkTestBase {
		protected AuthenticatorTestBase(NetworkStoreType storeType) 
			: base(storeType) {
		}

		protected abstract IAuthenticator CreateAuthenticator();
	}
}