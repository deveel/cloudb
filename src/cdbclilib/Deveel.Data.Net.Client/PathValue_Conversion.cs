using System;

namespace Deveel.Data.Net.Client {
	public sealed partial class PathValue {
		private static void CheckNullInvalidCast(PathValue value) {
			if (value == null)
				throw new InvalidCastException();
		}

		public static implicit operator bool (PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToBoolean();
		}

		public static implicit operator byte (PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToByte();
		}

		public static implicit operator short (PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToInt16();
		}

		public static implicit operator int (PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToInt32();
		}

		public static implicit operator long (PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToInt64();
		}

		public static implicit operator float (PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToSingle();
		}

		public static implicit operator double(PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToDouble();
		}

		public static implicit operator DateTime(PathValue value) {
			CheckNullInvalidCast(value);
			return value.ToDateTime();
		}

		public static implicit operator String(PathValue value) {
			return value == null ? null : value.ToString();
		}

		//TODO: invert implicit conversions ...
	}
}