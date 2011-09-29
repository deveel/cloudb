using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

using NUnit.Framework;

namespace Deveel.Data.Net.Security {
	[TestFixture]
	public abstract class AuthenticatorTestBase : NetworkTestBase {
		protected AuthenticatorTestBase(NetworkStoreType storeType) 
			: base(storeType) {
		}

		protected abstract Type AuthenticatorType { get; }

		protected override void Config(ConfigSource config) {
			config.SetValue("auth.type", AuthenticatorType.AssemblyQualifiedName);
			OnAuthenticatorConfig(config);
		}

		protected virtual void OnAuthenticatorConfig(ConfigSource config) {
		}

		protected abstract void PopulateAuthenticationData(IDictionary<string, AuthObject> authData);

		[Test]
		public void Authenticate() {
			IDictionary<string, AuthObject> authData = new Dictionary<string, AuthObject>();
			PopulateAuthenticationData(authData);
		}
	}
}