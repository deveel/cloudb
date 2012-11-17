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

namespace Deveel.Data {
	/// <summary>
	/// The base abstract definition of a key.
	/// </summary>
	[Serializable]
	public abstract class KeyBase : IComparable, IComparable<KeyBase> {
		protected KeyBase(short type, int secondary, long primary) {
			this.type = type;
			this.primary = primary;
			this.secondary = secondary;
		}

		internal KeyBase(long encodedV1, long encodedV2) {
			type = (short) (encodedV1 >> 32);
			secondary = (int) (encodedV1 & 0x0FFFFFFFF);
			primary = encodedV2;
		}

		private readonly short type;
		private readonly long primary;
		private readonly int secondary;

		/// <summary>
		/// Gets the secondary component of the key.
		/// </summary>
		public int Secondary {
			get { return secondary; }
		}

		/// <summary>
		/// Gets the primary component of the key.
		/// </summary>
		public long Primary {
			get { return primary; }
		}

		/// <summary>
		/// Gets the type of the key.
		/// </summary>
		public short Type {
			get { return type; }
		}

		public override bool Equals(object obj) {
			if (!(obj is KeyBase))
				throw new ArgumentException();

			KeyBase destKey = (KeyBase)obj;
			return destKey.type == type &&
			       destKey.secondary == secondary &&
			       destKey.primary == primary;
		}

		public override int GetHashCode() {
			return (int)((secondary << 6) + (type << 3) + primary);
		}

		public long GetEncoded(int n) {
			if (n == 1) {
				long v = (type & 0x0FFFFFFFFL) << 32;
				v |= (secondary & 0x0FFFFFFFFL);
				return v;
			} 
			if (n == 2) {
				return primary;
			}

			throw new ArgumentOutOfRangeException("n");
		}

		public int CompareTo(KeyBase other) {
			// Either this key or the compared key are not special case, so collate
			// on the key values,

			// Compare secondary keys
			int c = secondary < other.secondary ? -1 : (secondary == other.secondary ? 0 : 1);
			if (c == 0) {
				// Compare types
				c = type < other.type ? -1 : (type == other.type ? 0 : 1);
				if (c == 0) {
					// Compare primary keys
					if (primary > other.primary)
						return +1;
					if (primary < other.primary)
						return -1;
					return 0;
				}
			}
			return c;
		}

		public virtual int CompareTo(object obj) {
			if (!(obj is KeyBase))
				throw new ArgumentException();

			return CompareTo((KeyBase) obj);
		}
	}
}