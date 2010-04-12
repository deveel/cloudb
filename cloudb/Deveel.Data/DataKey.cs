using System;
using System.Text;

namespace Deveel.Data {
	[Serializable]
	public sealed class DataKey : KeyBase {
		public DataKey(short type, int secondary, long primary) 
			: base(type, secondary, primary) {
		}

		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("(");
			buf.Append(Secondary);
			buf.Append("-");
			buf.Append(Type);
			buf.Append("-");
			buf.Append(Primary);
			buf.Append(")");
			return buf.ToString();
		}
	}
}