using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;

namespace Deveel.Data.Net.Security {
	[Serializable]
	public sealed class AuthObject : ICloneable, ISerializable, IEnumerable<AuthObject> {
		private readonly AuthDataType dataType;
		private object value;

		private bool isList;
		private bool isGenericList;
		private AuthDataType listType;
		private readonly List<AuthObject> list;

		private AuthObject(SerializationInfo info, StreamingContext context) {
			//TODO:
		}

		public AuthObject(AuthDataType dataType, object value) {
			this.dataType = dataType;

			isList = dataType == AuthDataType.List;
			if (!isList) {
				this.value = value;
			} else {
				list = new List<AuthObject>();
				list.Add(new AuthObject(value));
			}
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

		public AuthObject(AuthDataType dataType) {
			this.dataType = dataType;

			if (dataType == AuthDataType.List) {
				list = new List<AuthObject>();
				isList = true;
			}
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

		public int Count {
			get {
				if (!isList)
					throw new InvalidOperationException("This object is not a list.");

				return list.Count;
			}
		}

		public AuthObject this[int index] {
			get {
				if (!isList)
					throw new InvalidOperationException("This object is not a list.");

				return list[index];
			}
		}

		public void Add(AuthObject item) {
			if (!isList)
				throw new InvalidOperationException("This object is not a list.");
			if (item != null && item.DataType != listType)
				throw new ArgumentException("The item is not null and its type is not accepted by the list.");

			list.Add(item);
		}

		public void Add(object item) {
			Add(new AuthObject(value));
		}

		public void RemoveAt(int index) {
			if (!isList)
				throw new InvalidOperationException("This object is not a list.");

			list.RemoveAt(index);
		}

		public IList<AuthObject> ToList() {
			if (!isList)
				throw new InvalidOperationException("The value of this object is not a valid list.");

			throw new NotImplementedException();
		}

		public object Clone() {
			AuthObject obj = new AuthObject(dataType);
			obj.isList = isList;
			obj.isGenericList = isGenericList;
			obj.listType = listType;

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

		public static AuthObject List(AuthDataType listType) {
			AuthObject list = new AuthObject(AuthDataType.List);
			list.listType = listType;
			return list;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			//TODO:
		}

		public IEnumerator<AuthObject> GetEnumerator() {
			if (!isList)
				throw new InvalidOperationException("This object is not a list.");

			return list.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}