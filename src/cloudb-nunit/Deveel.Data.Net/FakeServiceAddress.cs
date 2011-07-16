using System;

namespace Deveel.Data.Net {
	public sealed class FakeServiceAddress : IServiceAddress {
		public static readonly FakeServiceAddress Local = new FakeServiceAddress();
		
		public override bool Equals(object obj) {
			return obj is FakeServiceAddress;
		}
		
		public override int GetHashCode() {
			return base.GetHashCode();
		}
		
		public int CompareTo(IServiceAddress other) {
			return 0;
		}

		public override string ToString() {
			return "@FAKE@";
		}
	}
}