using System;
using System.Collections;
using System.Text;

namespace Deveel.Json {
	class JSONArray {
		private ArrayList list;

		public JSONArray() {
			list = new ArrayList();
		}

		public JSONArray(JSONReader x)
			: this() {
			char c = x.ReadCharClean();
			char q;
			if (c == '[') {
				q = ']';
			} else if (c == '(') {
				q = ')';
			} else {
				throw x.SyntaxError("A JSONArray text must start with '['");
			}
			if (x.ReadCharClean() == ']') {
				return;
			}
			x.ReadBack();
			for (; ; ) {
				if (x.ReadCharClean() == ',') {
					x.ReadBack();
					list.Add(null);
				} else {
					x.ReadBack();
					list.Add(x.ReadValue());
				}
				c = x.ReadCharClean();
				switch (c) {
					case ';':
					case ',':
						if (x.ReadCharClean() == ']')
							return;
						x.ReadBack();
						break;
					case ']':
					case ')':
						if (q != c)
							throw x.SyntaxError("Expected a '" + q + "'");
						return;
					default:
						throw x.SyntaxError("Expected a ',' or ']'");
				}
			}
		}


		public JSONArray(String source)
			: this(new JSONReader(source)) {
		}

		public JSONArray(ICollection collection) {
			list = new ArrayList();
			if (collection != null) {
				foreach (object o in collection) {
					list.Add(JSONObject.Wrap(o));
				}
			}
		}


		public JSONArray(object array)
			: this() {
			if (array.GetType().IsArray) {
				int length = ((Array)array).Length;
				for (int i = 0; i < length; i += 1) {
					Add(JSONObject.Wrap(((Array)array).GetValue(i)));
				}
			} else {
				throw new JSONException("JSONArray initial value should be a string or collection or array.");
			}
		}

		public object this[int index] {
			get { return GetValue(index); }
			set { SetValue(index, value); }
		}

		public bool IsNull(int index) {
			return JSONObject.Null.Equals(GetValue(index));
		}


		public string Join(string separator) {
			int len = Length;
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < len; i += 1) {
				if (i > 0) {
					sb.Append(separator);
				}
				sb.Append(JSONObject.ValueToString(list[i]));
			}
			return sb.ToString();
		}

		public int Length {
			get { return list.Count; }
		}

		public object GetValue(int index) {
			return (index < 0 || index >= Length) ? null : list[index];
		}

		public JSONArray Add(Object value) {
			JSONObject.CheckValid(value);
			list.Add(value);
			return this;
		}

		public JSONArray SetValue(int index, Object value) {
			JSONObject.CheckValid(value);
			if (index < 0) {
				throw new JSONException("JSONArray[" + index + "] not found.");
			}
			if (index < Length) {
				list[index] = value;
			} else {
				while (index != Length) {
					Add(JSONObject.Null);
				}
				Add(value);
			}
			return this;
		}


		public object RemoveAt(int index) {
			object o = GetValue(index);
			list.RemoveAt(index);
			return o;
		}


		public JSONObject ToJSONObject(JSONArray names) {
			if (names == null || names.Length == 0 || Length == 0)
				return null;

			JSONObject jo = new JSONObject();
			for (int i = 0; i < names.Length; i += 1) {
				jo.SetValue(names.GetValue(i).ToString(), GetValue(i));
			}
			return jo;
		}


		public override String ToString() {
			return '[' + Join(",") + ']';
		}


		public String ToString(int indentFactor) {
			return ToString(indentFactor, 0);
		}


		internal String ToString(int indentFactor, int indent) {
			int len = Length;
			if (len == 0) {
				return "[]";
			}
			int i;
			StringBuilder sb = new StringBuilder("[");
			if (len == 1) {
				sb.Append(JSONObject.ValueToString(list[0], indentFactor, indent));
			} else {
				int newindent = indent + indentFactor;
				sb.Append('\n');
				for (i = 0; i < len; i += 1) {
					if (i > 0) {
						sb.Append(",\n");
					}
					for (int j = 0; j < newindent; j += 1) {
						sb.Append(' ');
					}
					sb.Append(JSONObject.ValueToString(list[i], indentFactor, newindent));
				}
				sb.Append('\n');
				for (i = 0; i < indent; i += 1) {
					sb.Append(' ');
				}
			}
			sb.Append(']');
			return sb.ToString();
		}
	}
}