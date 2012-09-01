using System;

namespace Deveel.Data.Net {
	public sealed class DataAddress {
		private readonly NodeId value;

		internal DataAddress(NodeId value) {
			this.value = value;
		}

		internal DataAddress(BlockId blockId, int dataId) {
			// TODO: Check for overflow?
			long[] blockAddr = blockId.ReferenceAddress;
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
			get { return ((int) value.Low) & 0x0FFFF; }
		}

		public DataAddress Max(DataAddress address) {
			if (Value.CompareTo(address.Value) >= 0)
				return this;

			return address;
		}

		public override string ToString() {
			return Value.ToString();
		}

		public override int GetHashCode() {
			return Value.GetHashCode();
		}

		public override bool Equals(object obj) {
			DataAddress other = obj as DataAddress;
			if (other == null)
				return false;

			return Value.Equals(other.Value);
		}

		public static DataAddress Parse(String s) {
			return new DataAddress(NodeId.Parse(s));
		}
	}
}