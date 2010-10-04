using System;
using System.Globalization;

namespace Deveel.Data.Net {
	public sealed class MethodArgument : ICloneable, IConvertible {
		private readonly string name;
		private object value;
		private readonly bool readOnly;
		private ArgumentList children;
		private string format;

		internal MethodArgument(string name, object value, bool readOnly) {
			this.name = name;
			this.value = value;
			this.readOnly = readOnly;
			children = new ArgumentList(readOnly);
		}

		public MethodArgument(string name, object value)
			: this(name, value, false) {
		}

		public MethodArgument(string name)
			: this(name, null) {
		}

		public string Name {
			get { return name; }
		}

		public object Value {
			get { return value; }
			set {
				CheckReadOnly();
				this.value = value;
			}
		}

		public ArgumentList Children {
			get { return children; }
		}

		public string Format {
			get { return format; }
			set { format = value; }
		}

		private void CheckReadOnly() {
			if (readOnly)
				throw new InvalidOperationException("The argument cannot be written.");
		}

		public object Clone() {
			object newValue = Value;
			if (newValue is ICloneable)
				newValue = ((ICloneable) newValue).Clone();

			MethodArgument arg = new MethodArgument(Name, newValue, readOnly);
			arg.children = (ArgumentList) children.Clone();
			return arg;
		}

		TypeCode IConvertible.GetTypeCode() {
			return Convert.GetTypeCode(value);
		}

		public bool ToBoolean() {
			return ToBoolean(CultureInfo.InvariantCulture);
		}

		public bool ToBoolean(IFormatProvider provider) {
			return Convert.ToBoolean(value, provider);
		}

		public char ToChar() {
			return ToChar(CultureInfo.InvariantCulture);
		}

		public char ToChar(IFormatProvider provider) {
			return Convert.ToChar(value, provider);
		}

		[CLSCompliant(false)]
		public sbyte ToSByte() {
			return ToSByte(CultureInfo.InvariantCulture);
		}

		[CLSCompliant(false)]
		public sbyte ToSByte(IFormatProvider provider) {
			return Convert.ToSByte(value, provider);
		}

		public byte ToByte() {
			return ToByte(CultureInfo.InvariantCulture);
		}

		public byte ToByte(IFormatProvider provider) {
			return Convert.ToByte(value, provider);
		}

		public short ToInt16() {
			return ToInt16(CultureInfo.InvariantCulture);
		}

		public short ToInt16(IFormatProvider provider) {
			return Convert.ToInt16(value, provider);
		}

		[CLSCompliant(false)]
		public ushort ToUInt16() {
			return ToUInt16(CultureInfo.InvariantCulture);
		}

		[CLSCompliant(false)]
		public ushort ToUInt16(IFormatProvider provider) {
			return Convert.ToUInt16(provider);
		}

		public int ToInt32() {
			return ToInt32(CultureInfo.InvariantCulture);
		}

		public int ToInt32(IFormatProvider provider) {
			return Convert.ToInt32(value, provider);
		}

		[CLSCompliant(false)]
		public uint ToUInt32() {
			return ToUInt32(CultureInfo.InvariantCulture);
		}

		[CLSCompliant(false)]
		public uint ToUInt32(IFormatProvider provider) {
			return Convert.ToUInt32(value, provider);
		}

		public long ToInt64() {
			return ToInt64(CultureInfo.InvariantCulture);
		}

		public long ToInt64(IFormatProvider provider) {
			return Convert.ToInt64(value, provider);
		}

		[CLSCompliant(false)]
		public ulong ToUInt64() {
			return ToUInt64(CultureInfo.InvariantCulture);
		}

		[CLSCompliant(false)]
		public ulong ToUInt64(IFormatProvider provider) {
			return Convert.ToUInt64(value, provider);
		}

		public float ToSingle() {
			return ToSingle(CultureInfo.InvariantCulture);
		}

		public float ToSingle(IFormatProvider provider) {
			return Convert.ToSingle(value, provider);
		}

		public double ToDouble() {
			return ToDouble(CultureInfo.InvariantCulture);
		}

		public double ToDouble(IFormatProvider provider) {
			return Convert.ToDouble(value, provider);
		}

		public decimal ToDecimal() {
			return ToDecimal(CultureInfo.InvariantCulture);
		}

		public decimal ToDecimal(IFormatProvider provider) {
			return Convert.ToDecimal(value, provider);
		}

		public DateTime ToDateTime() {
			return ToDateTime(CultureInfo.InvariantCulture);
		}

		public DateTime ToDateTime(IFormatProvider provider) {
			return Convert.ToDateTime(value, provider);
		}

		public override string ToString() {
			return ToString(CultureInfo.InvariantCulture);
		}

		public string ToString(IFormatProvider provider) {
			return Convert.ToString(value, provider);
		}

		public object ToType(Type conversionType, IFormatProvider provider) {
			throw new NotImplementedException();
		}
	}
}