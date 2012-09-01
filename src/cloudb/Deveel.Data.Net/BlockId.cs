using System;

namespace Deveel.Data.Net {
	public class BlockId : Quadruple {
		public BlockId(long high, long low)
			: base(high, low) {
		}

		public long[] ReferenceAddress {
			get {
				long[] address = new long[2];
				address[0] = (High << 16) | ((Low >> 48) & 0x0FFFF);
				address[1] = Low << 16;
				return address;
			}
		}

		public BlockId Add(int value) {
			if (value < 0)
				throw new ArgumentException("positive_val < 0");

			long low = Low + value;
			long high = High;
			// If the new low value is positive, and the old value was negative,
			if (low >= 0 && Low < 0) {
				// We overflowed, so add 1 to the high val,
				++high;
			}
			return new BlockId(high, low);
		}
	}
}