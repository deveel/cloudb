using System;

namespace Deveel.Data {
	public class Quadruple : IComparable<Quadruple> {
		private readonly long high;
		private readonly long low;

		public Quadruple(long high, long low) {
			this.high = high;
			this.low = low;
		}

		public Quadruple(long[] refs)
			: this(refs[0], refs[1]) {
		}

		public long Low {
			get { return low; }
		}

		public long High {
			get { return high; }
		}

		public override int GetHashCode() {
			return (int) (low & 0x07FFFFFFFL);
		}

		public override bool Equals(object obj) {
			if (obj == this)
				return true;

			Quadruple other = obj as Quadruple;
			if (other == null)
				return false;

			if (low == other.low &&
			    high == other.high) {
				return true;
			}
			return false;
		}

		public int CompareTo(Quadruple other) {
			if (high < other.high)
				return -1;
			if (high > other.high)
				return 1;

			// High 64-bits are equal, so compare low,

			// This comparison needs to be unsigned,
			// True if the signs are different
			bool signdif = (low < 0) != (other.low < 0);

			if ((low < other.low) ^ signdif)
				return -1;
			if ((low > other.low) ^ signdif)
				return 1;
			// Equal,
			return 0;
		}
	}
}