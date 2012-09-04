using System;

using Deveel.Data.Net;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class CloudBaseTestBase : PathTestBase {
		private DbSession session;

		protected CloudBaseTestBase(StoreType storeType) 
			: base(storeType) {
		}

		protected override string PathType {
			get { return "Deveel.Data.CloudBasePath, cloudbase"; }
		}

		protected DbSession Session {
			get { return session; }
		}

		protected override void OnSetUp() {
			base.OnSetUp();

			session = new DbSession(Client, PathName);
		}
	}
}