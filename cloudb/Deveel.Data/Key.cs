using System;
using System.Text;

namespace Deveel.Data {
	[Serializable]
	public sealed class Key : KeyBase {
		public Key(short type, long primary, int secondary)
			: base(type, secondary, primary) {
		}

		internal Key(long encoded_v1, long encoded_v2)
			: base(encoded_v1, encoded_v2) {
		}

		internal const short SpecialKeyType = 0x07F80;

		public static readonly Key Head = new Key(SpecialKeyType, -1, -2);
		public static readonly Key Tail = new Key(SpecialKeyType, -1, -1);

		public override int CompareTo(object obj) {
			if (!(obj is Key))
				throw new ArgumentException();

			Key key = (Key)obj;
			// Handle the special case head and tail keys,
			if (Type == SpecialKeyType) {
				// This is special case,
				if (Equals(Head))
					// This is less than any other key except head which it is equal to
					return key.Equals(Head) ? 0 : -1;
				// Must be a tail key,
				if (Equals(Tail))
					// This is greater than any other key except tail which it is equal to
					return key.Equals(Tail) ? 0 : 1;

				throw new ApplicationException("Unknown special case key");
			}
			if (key.Type == SpecialKeyType) {
				// This is special case,
				if (key.Equals(Head))
					// Every key is greater than head except head which it is equal to
					return Equals(Head) ? 0 : 1;

				// Must be a tail key,
				if (key.Equals(Tail))
					// Every key is less than tail except tail which it is equal to
					return Equals(Tail) ? 0 : -1;

				throw new ApplicationException("Unknown special case key");
			}

			// Either this key or the compared key are not special case, so collate
			// on the key values,

			return base.CompareTo(obj);
		}

		public override string ToString() {
			if (Equals(Head))
				return "HEAD";
			if (Equals(Tail))
				return "TAIL";
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