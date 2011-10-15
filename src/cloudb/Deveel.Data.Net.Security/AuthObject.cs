using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net.Security {
	public sealed class AuthObject : ICloneable {
		private readonly AuthDataType dataType;
		private object value;

		private bool isList;
		private bool isGenericList;
		private AuthDataType listType;

		public AuthObject(AuthDataType dataType, object value) {
			this.dataType = dataType;
			this.value = value;
		}

		public AuthObject(object value) {
			if (value == null) {
				dataType = AuthDataType.Null;
			} else if (value is byte[] ||
				value is Stream) {
				dataType = AuthDataType.Binary;
			} else if (value is IList) {
				isList = true;
			} else if (typeof (IList<>).IsInstanceOfType(value)) {
				isList = true;
				isGenericList = true;
			} else if (value is string) {
				dataType = AuthDataType.String;
			} else if (value is bool) {
				dataType = AuthDataType.Boolean;
			} else if (value is byte ||
			           value is short ||
			           value is int ||
			           value is long ||
			           value is float ||
			           value is double) {
				dataType = AuthDataType.Number;
			} else if (value is DateTime) {
				dataType = AuthDataType.DateTime;
			}

			this.value = value;
		}

		public AuthObject(AuthDataType dataType)
			: this(dataType, null) {
		}

		public bool IsList {
			get { return isList; }
		}

		public object Value {
			get { return value; }
		}

		public AuthDataType DataType {
			get { return dataType; }
		}

		public AuthDataType ElementType {
			get { return listType; }
		}

		public IList<T> ToList<T>() {
			if (!isList)
				throw new InvalidOperationException("The value of this object is not a valid list.");

			throw new NotImplementedException();
		}

		public IList ToList() {
			throw new NotImplementedException();
		}

		public object Clone() {
			AuthObject obj = new AuthObject(dataType) {
				isList = isList,
				isGenericList = isGenericList,
				listType = listType
			};

			if (value == null || !(value is ICloneable)) {
				obj.value = value;
			} else {
				obj.value = ((ICloneable)value).Clone();
			}

			return obj;
		}

		public static implicit operator AuthObject(string value) {
			return new AuthObject(value);
		}

		public static implicit operator AuthObject(bool value) {
			return new AuthObject(value);
		}

		public static implicit operator AuthObject(int value) {
			return new AuthObject(value);
		}

		public static implicit operator AuthObject(short value) {
			return new AuthObject(value);
		}
	}
}