using System;

namespace Deveel.Data {
	/// <summary>
	/// An addressable entity that is represented as a 108-bit value.
	/// </summary>
	public sealed class BlockId : Quadruple {
		public BlockId(long high, long low) 
			: base(high, low) {
		}

		public long[] Address {
			get {
				long[] result = new long[2];
				result[0] = (High << 16) | ((Low >> 48) & 0x0FFFF);
				result[1] = Low << 16;
				return result;
			}
		}

		public BlockId Add(int value) {
			if (value < 0)
				throw new ArgumentOutOfRangeException("value");

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