using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	public sealed class DbRootAddress {
		private readonly DbSession session;
		private readonly DataAddress address;

		internal DbRootAddress(DbSession session, DataAddress address) {
			this.session = session;
			this.address = address;
		}

		internal DbSession Session {
			get { return session; }
		}

		internal DataAddress Address {
			get { return address; }
		}
	}
}