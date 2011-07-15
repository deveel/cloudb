using System;

namespace Deveel.Data.Net.Client {
	[Serializable]
	public sealed partial class PathValue : IConvertible {
		private readonly object value;
		private readonly Array array;
		private readonly Type valueType;
		private readonly PathValueType valueTypeCode;

		public static readonly PathValue Null = new PathValue(null);

		internal PathValue(object value) {
			valueTypeCode = GetTypeCode(value, out valueType);
			if (valueTypeCode == PathValueType.Array) {
				array = (Array) value;
			} else if (valueTypeCode == PathValueType.Struct) {
				value = PathStruct.FromObject(value);
			}

			this.value = value;
		}

		internal object Value {
			get { return value; }
		}

		public int Length {
			get {
				CheckIsArray();
				return array.Length;
			}
		}

		public bool IsNull {
			get { return valueTypeCode == PathValueType.Null; }
		}

		public bool IsStruct {
			get { return valueTypeCode == PathValueType.Struct; }
		}

		public bool IsArray {
			get { return valueTypeCode == PathValueType.Array; }
		}

		internal PathValueType ValueType {
			get { return valueTypeCode; }
		}

		private static PathValueType GetTypeCode(object value, out Type type) {
			if (value == null || DBNull.Value == value) {
				type = typeof (DBNull);
				return PathValueType.Null;
			}

			type = value.GetType();
			if (type.IsArray) {
				type = type.GetElementType();
				//TODO: check if the element type is allowed ...
				return PathValueType.Array;
			}

			if (value is bool)
				return PathValueType.Boolean;
			if (value is byte)
				return PathValueType.Byte;
			if (value is short)
				return PathValueType.Int16;
			if (value is int)
				return PathValueType.Int32;
			if (value is long)
				return PathValueType.Int64;
			if (value is float)
				return PathValueType.Single;
			if (value is double)
				return PathValueType.Double;
			if (value is DateTime)
				return PathValueType.DateTime;
			if (value is String)
				return PathValueType.String;

			if (type.IsPrimitive)
				throw new ArgumentException("The primitive type '" + type + "' is not supported.");

			return PathValueType.Struct;
		}

		private void CheckIsArray() {
			if (valueTypeCode != PathValueType.Array)
				throw new ArgumentException("The attribute is not an array.");
		}

		TypeCode IConvertible.GetTypeCode() {
			if (valueTypeCode == PathValueType.Boolean)
				return TypeCode.Boolean;
			if (valueTypeCode == PathValueType.Byte)
				return TypeCode.Byte;
			if (valueTypeCode == PathValueType.Int16)
				return TypeCode.Int16;
			if (valueTypeCode == PathValueType.Int32)
				return TypeCode.Int32;
			if (valueTypeCode == PathValueType.Int64)
				return TypeCode.Int64;
			if (valueTypeCode == PathValueType.Single)
				return TypeCode.Single;
			if (valueTypeCode == PathValueType.Double)
				return TypeCode.Double;
			if (valueTypeCode == PathValueType.String)
				return TypeCode.String;
			if (valueTypeCode == PathValueType.DateTime)
				return TypeCode.DateTime;

			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToInt16();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToInt64();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return ToSingle();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToDouble();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			return ToDateTime();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			throw new NotImplementedException();
		}

		private bool ToBoolean() {
			throw new NotImplementedException();
		}

		private byte ToByte() {
			throw new NotImplementedException();
		}

		private short ToInt16() {
			throw new NotImplementedException();
		}

		private int ToInt32() {
			throw new NotImplementedException();
		}

		private long ToInt64() {
			throw new NotImplementedException();
		}

		private float ToSingle() {
			throw new NotImplementedException();
		}

		private double ToDouble() {
			throw new NotImplementedException();
		}

		private DateTime ToDateTime() {
			throw new NotImplementedException();
		}

		public override string ToString() {
			return base.ToString();
		}

		public void SetValue(int index, object value) {
			CheckIsArray();

			if (value != null && !valueType.IsInstanceOfType(value))
				throw new ArgumentException("The value is not assignable from the array element type.");

			array.SetValue(value, index);
		}

		public object GetValue(int index) {
			CheckIsArray();

			return array.GetValue(index);
		}

		public override bool Equals(object obj) {
			PathValue other = obj as PathValue;
			if (other == null)
				return false;

			if (IsArray) {
				if (!other.IsArray)
					return false;

				if (!valueType.Equals(other.valueType))
					return false;

				if (array.Length != other.array.Length)
					return false;

				for (int i = 0; i < array.Length; i++) {
					object value1 = array.GetValue(i);
					object value2 = other.array.GetValue(i);

					if (value1 == null && value2 == null)
						continue;

					if (value1 != null && !value1.Equals(value2))
						return false;

				}

				return true;
			}

			if (valueTypeCode != other.valueTypeCode)
				return false;

			if (value == null && other.value == null)
				return true;
			if (value == null)
				return false;

			return value.Equals(other.value);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public static PathValue CreateArray(PathValueType valueType, int length) {
			if (valueType == PathValueType.Array)
				throw new ArgumentException("Arrays of arrays are not supported.");
			if (valueType == PathValueType.Null)
				throw new ArgumentException("Nulls are not valid arrays element types.");

			Type elementType;
			if (valueType == PathValueType.Boolean)
				elementType = typeof(bool);
			else if (valueType == PathValueType.Byte)
				elementType = typeof(byte);
			else if (valueType == PathValueType.Int16)
				elementType = typeof(short);
			else if (valueType == PathValueType.Int32)
				elementType = typeof(int);
			else if (valueType == PathValueType.Int64)
				elementType = typeof(long);
			else if (valueType == PathValueType.Single)
				elementType = typeof(float);
			else if (valueType == PathValueType.Double)
				elementType = typeof(double);
			else if (valueType == PathValueType.DateTime)
				elementType = typeof(DateTime);
			else if (valueType == PathValueType.String)
				elementType = typeof(string);
			else
				elementType = typeof (object);

			return new PathValue(Array.CreateInstance(elementType, length));
		}
	}
}