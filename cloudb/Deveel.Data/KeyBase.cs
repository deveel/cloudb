using System;

namespace Deveel.Data {
	[Serializable]
	public abstract class KeyBase : IComparable {
		protected KeyBase(short type, int secondary, long primary) {
			this.type = type;
			this.primary = primary;
			this.secondary = secondary;
		}

		internal KeyBase(long encoded_v1, long encoded_v2) {
			type = (short) (encoded_v1 >> 32);
			secondary = (int) (encoded_v1 & 0x0FFFFFFFF);
			primary = encoded_v2;
		}

		private readonly short type;
		private readonly long primary;
		private readonly int secondary;

		public int Secondary {
			get { return secondary; }
		}

		public long Primary {
			get { return primary; }
		}

		public short Type {
			get { return type; }
		}

		public override bool Equals(object obj) {
			if (!(obj is KeyBase))
				throw new ArgumentException();

			KeyBase dest_key = (KeyBase)obj;
			return dest_key.type == type &&
			       dest_key.secondary == secondary &&
			       dest_key.primary == primary;
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

		public virtual int CompareTo(object obj) {
			if (!(obj is KeyBase))
				throw new ArgumentException();

			KeyBase key = (KeyBase)obj;

			// Either this key or the compared key are not special case, so collate
			// on the key values,

			// Compare secondary keys
			int c = (secondary - key.secondary);
			if (c == 0) {
				// Compare types
				c = (type - key.type);
				if (c == 0) {
					// Compare primary keys
					if (primary > key.primary)
						return +1;
					if (primary < key.primary)
						return -1;
					return 0;
				}
			}
			return c;
		}
	}
}