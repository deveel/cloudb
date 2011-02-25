using System;

namespace Deveel.Data.Net {
	public sealed class DataAddress {
		private readonly NodeId value;

		public DataAddress(NodeId value) {
			this.value = value;
		}

		public DataAddress(BlockId blockId, int dataId) {
			//TODO: Check for overflow?
			long[] blockAddr = blockId.Address;
			blockAddr[1] |= dataId & 0x0FFFF;
			value = new NodeId(blockAddr);
		}

		public NodeId Value {
			get { return value; }
		}

		public BlockId BlockId {
			get {
				long addrLow = value.Low;
				long addrHigh = value.High;
				addrLow = (addrLow >> 16) & 0x0FFFFFFFFFFFFL;
				addrLow |= (addrHigh & 0x0FF) << 48;
				addrHigh = addrHigh >> 16;

				return new BlockId(addrHigh, addrLow);
			}
		}

		public int DataId {
			get { return ((int)value.Low) & 0x0FFFF; }
		}

		public DataAddress Max(DataAddress address) {
			return Value.CompareTo(address.Value) >= 0 ? this : address;
		}

		public override bool Equals(object obj) {
			if (this == obj)
				return true;
			if (!(obj is DataAddress))
				return false;
			return Value.Equals(((DataAddress) obj).Value);
		}

		public override int GetHashCode() {
			return Value.GetHashCode();
		}

		public override string ToString() {
			return Value.ToString();
		}

		public static DataAddress Parse(string s) {
			return new DataAddress(NodeId.Parse(s));
		}
	}
}