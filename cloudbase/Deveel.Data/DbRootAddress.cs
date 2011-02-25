using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	public class DbRootAddress {
		private readonly DbSession session;
		private readonly DataAddress address;

		internal DbRootAddress(DbSession session, DataAddress address) {
			this.address = address;
			this.session = session;
		}

		internal DbSession Session {
			get { return session; }
		}

		internal DataAddress Address {
			get { return address; }
		}

		public int DataId {
			get { return address.DataId; }
		}

		public BlockId BlockId {
			get { return address.BlockId; }
		}
	}
}