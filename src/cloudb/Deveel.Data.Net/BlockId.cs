//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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