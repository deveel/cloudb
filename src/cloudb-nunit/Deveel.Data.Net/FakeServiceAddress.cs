using System;

namespace Deveel.Data.Net {
	public sealed class FakeServiceAddress : IServiceAddress {
		private readonly string id;

		public static readonly FakeServiceAddress Local1 = new FakeServiceAddress("1");
		public static readonly FakeServiceAddress Local2 = new FakeServiceAddress("2");

		public FakeServiceAddress(string id) {
			this.id = id;
		}

		public string Id {
			get { return id; }
		}

		public override bool Equals(object obj) {
			return obj is FakeServiceAddress && (id.Equals(((FakeServiceAddress)obj).id));
		}
		
		public override int GetHashCode() {
			return id.GetHashCode();
		}
		
		public int CompareTo(IServiceAddress other) {
			FakeServiceAddress address = (FakeServiceAddress) other;
			return id.CompareTo(address.id);
		}

		public override string ToString() {
			return String.Format("@FAKE{0}@", id);
		}
	}
}