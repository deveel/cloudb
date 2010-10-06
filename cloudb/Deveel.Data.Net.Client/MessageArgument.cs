using System;
using System.Globalization;

namespace Deveel.Data.Net.Client {
	public sealed class MessageArgument : ICloneable, IConvertible, IAttributesHandler {
		private readonly string name;
		private object value;
		private bool readOnly;
		private MessageArguments children;
		private MessageAttributes attributes;
		private string idKey;
		private string format;

		internal MessageArgument(string name, object value, bool readOnly) {
			this.name = name;
			this.value = value;
			this.readOnly = readOnly;
			children = new MessageArguments(readOnly);
			attributes = new MessageAttributes(this);
		}

		public MessageArgument(string name, object value)
			: this(name, value, false) {
		}

		public MessageArgument(string name)
			: this(name, null) {
		}

		public string Name {
			get { return name; }
		}

		public bool HasId {
			get { return idKey != null && attributes.Contains(idKey); }
		}

		public object Id {
			get { return idKey == null ? null : attributes[idKey]; }
		}

		public bool IsId(string key) {
			return idKey != null && idKey == key;
		}

		public void SetId(string key, object id) {
			if (idKey != null)
				throw new ArgumentException("A unique identifier for this argument was already set.");

			if (attributes.Contains(key))
				throw new ArgumentException("The key for the unique identifier is already in use.");

			idKey = key;
			attributes[key] = id;
		}

		public object Value {
			get { return value; }
			set {
				CheckReadOnly();
				this.value = value;
			}
		}

		public MessageArguments Children {
			get { return children; }
		}

		public MessageAttributes Attributes {
			get { return attributes; }
		}

		bool IAttributesHandler.IsReadOnly {
			get { return readOnly; }
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

			MessageArgument arg = new MessageArgument(Name, newValue, readOnly);
			arg.children = (MessageArguments) children.Clone();
			arg.attributes = (MessageAttributes) attributes.Clone();
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

		internal void Seal() {
			readOnly = true;
		}
	}
}