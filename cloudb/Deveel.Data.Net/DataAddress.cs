using System;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class DataAddress {
		private readonly long value;

		public DataAddress(long value) {
			this.value = value;
		}

		public DataAddress(long blockId, int dataId) {
			//TODO: Check for overflow?
			//TODO: check.. long v1 = (blockId << 14) & 0x0FFFFFFFFFFFFC000L;
			long v1 = (blockId << 14) & -16384;
			int v2 = dataId & 0x0FFFF;
			value = v1 | v2;
		}

		public long Value {
			get { return value; }
		}

		public long BlockId {
			get { return (value >> 14) & 0x00003FFFFFFFFFFFFL; }
		}

		public int DataId {
			get { return ((int) value) & 0x03FFF; }
		}

		public DataAddress Max(DataAddress address) {
			return value >= address.value ? this : address;
		}

		public override bool Equals(object obj) {
			DataAddress d_addr = (DataAddress)obj;
			return (value == d_addr.value);
		}

		public override int GetHashCode() {
			return ((int)value) ^ (int)(value >> 32);
		}

		public override string ToString() {
			StringBuilder b = new StringBuilder();
			b.AppendFormat("0x{0:x}", BlockId);
			b.Append(".");
			b.AppendFormat("0x{0:x}", DataId);
			return b.ToString();
		}

		public static DataAddress Parse(string s) {
			int delim = s.IndexOf(".");
			String bid = s.Substring(0, delim);
			String did = s.Substring(delim + 1);
			return new DataAddress(Convert.ToInt64(bid, 16), Convert.ToInt32(did, 16));
		}
	}
}