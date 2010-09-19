using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Deveel.Json {
	class JSONObject {
		private class NullValue : ICloneable {

			public object Clone() {
				return this;
			}


			public override bool Equals(object obj) {
				return obj == null || obj == this;
			}

			public override int GetHashCode() {
				return 0;
			}

			public override string ToString() {
				return "null";
			}
		}


		private readonly IDictionary<string, object> map;


		public static readonly object Null = new NullValue();


		public JSONObject() {
			map = new Dictionary<string, object>();
		}


		public JSONObject(JSONObject jo, string[] names)
			: this() {
			for (int i = 0; i < names.Length; i += 1) {
				if (!HasValue(names[i]))
					SetValue(names[i], jo.GetValue<object>(names[i]));
			}
		}


		public JSONObject(JSONReader x)
			: this() {
			char c;
			String key;

			if (x.ReadCharClean() != '{') {
				throw x.SyntaxError("A JSONObject text must begin with '{'");
			}
			for (; ; ) {
				c = x.ReadCharClean();
				switch (c) {
					case '\0':
						throw x.SyntaxError("A JSONObject text must end with '}'");
					case '}':
						return;
					default:
						x.ReadBack();
						key = x.ReadValue().ToString();
						break;
				}

				// The key is followed by ':'. We will also tolerate '=' or '=>'.

				c = x.ReadCharClean();
				if (c == '=') {
					if (x.ReadChar() != '>') {
						x.ReadBack();
					}
				} else if (c != ':') {
					throw x.SyntaxError("Expected a ':' after a key");
				}

				if (!HasValue(key))
					SetValue(key, x.ReadValue());

				// Pairs are separated by ','. We will also tolerate ';'.

				switch (x.ReadCharClean()) {
					case ';':
					case ',':
						if (x.ReadCharClean() == '}') {
							return;
						}
						x.ReadBack();
						break;
					case '}':
						return;
					default:
						throw x.SyntaxError("Expected a ',' or '}'");
				}
			}
		}

		public JSONObject(Object bean)
			: this() {
			populateMap(bean);
		}


		public JSONObject(IDictionary<string, object> map) {
			this.map = new Dictionary<string, object>();
			if (map != null) {
				foreach (KeyValuePair<string, object> pair in map) {
					this.map.Add(pair.Key, Wrap(pair.Value));
				}
			}
		}


		public JSONObject(object obj, string[] names)
			: this() {
			Type c = obj.GetType();
			for (int i = 0; i < names.Length; i += 1) {
				String name = names[i];
				object value = c.GetField(name).GetValue(obj);
				if (value != null)
					SetValue(name, value);
			}
		}


		public JSONObject(String source)
			: this(new JSONReader(source)) {
		}


		public JSONObject Accumulate(String key, Object value) {
			CheckValid(value);
			object o = GetValue<object>(key);
			if (o == null) {
				SetValue(key, value is JSONArray ? new JSONArray().Add(value) : value);
			} else if (o is JSONArray) {
				((JSONArray)o).Add(value);
			} else {
				SetValue(key, new JSONArray().Add(o).Add(value));
			}
			return this;
		}

		private void populateMap(Object bean) {
			throw new NotImplementedException();
		}


		public JSONObject Append(String key, Object value) {
			CheckValid(value);
			object o = GetValue<object>(key);
			if (o == null) {
				SetValue(key, new JSONArray().Add(value));
			} else if (o is JSONArray) {
				SetValue(key, ((JSONArray)o).Add(value));
			} else {
				throw new JSONException("JSONObject[" + key + "] is not a JSONArray.");
			}
			return this;
		}

		public static String[] GetNames(JSONObject jo) {
			int length = jo.Length;
			if (length == 0)
				return null;

			string[] names = new String[length];
			int j = 0;
			foreach(string key in jo.Keys) {
				names[j] = key;
				j += 1;
			}
			return names;
		}


		public static String[] GetNames(Object obj) {
			if (obj == null) {
				return null;
			}
			Type type = obj.GetType();
			FieldInfo[] fields = type.GetFields();
			int length = fields.Length;
			if (length == 0)
				return null;

			string[] names = new string[length];
			for (int i = 0; i < length; i += 1) {
				names[i] = fields[i].Name;
			}
			return names;
		}

		public bool HasValue(String key) {
			return map.ContainsKey(key);
		}


		public JSONObject Increment(String key) {
			object value = GetValue<object>(key);
			if (value == null) {
				SetValue(key, 1);
			} else {
				if (value is int) {
					SetValue(key, ((int)value) + 1);
				} else if (value is long) {
					SetValue(key, ((long)value) + 1);
				} else if (value is double) {
					SetValue(key, ((double)value) + 1);
				} else if (value is float) {
					SetValue(key, ((float)value) + 1);
				} else {
					throw new JSONException("Unable to increment [" + key + "].");
				}
			}
			return this;
		}

		public bool IsNull(String key) {
			return Null.Equals(GetValue<object>(key));
		}


		public ICollection<string > Keys {
			get { return map.Keys; }
		}

		public int Length {
			get { return map.Count; }
		}

		public JSONArray Names {
			get {
				JSONArray ja = new JSONArray();
				foreach(string key in Keys) {
					ja.Add(key);
				}
				return ja.Length == 0 ? null : ja;
			}
		}

		internal static string NumberToString(object n) {
			if (n == null)
				throw new JSONException("Null pointer");

			CheckValid(n);

			// Shave off trailing zeros and decimal point, if possible.

			string s = n.ToString();
			if (s.IndexOf('.') > 0 && s.IndexOf('e') < 0 && s.IndexOf('E') < 0) {
				while (s.EndsWith("0")) {
					s = s.Substring(0, s.Length - 1);
				}
				if (s.EndsWith(".")) {
					s = s.Substring(0, s.Length - 1);
				}
			}
			return s;
		}


		public T GetValue<T>(String key) {
			object value;
			if (!map.TryGetValue(key, out value))
				return default(T);
			if (typeof(T) == typeof(JSONObject)) {
				if (value is JSONObject)
					return (T)value;
				if (value is string)
					return (T)(object)(new JSONObject((string) value));
				throw new ArgumentException();
			}
			if (typeof(T) == typeof(JSONArray)) {
				if (value is JSONArray)
					return (T) value;
				if (value is string)
					return (T) (object) (new JSONArray((string) value));
				if (value is Array)
					return (T) (object) (new JSONArray(value));
				if (value is JSONObject)
					return (T) (object) new JSONArray(new JSONObject[] {(JSONObject) value});

				throw new ArgumentException();
			}
			return (T) Convert.ChangeType(value, typeof(T));
		}

		public JSONObject SetValue(String key, Object value) {
			if (key == null) {
				throw new JSONException("Null key.");
			}
			if (value != null) {
				CheckValid(value);
				map[key] = value;
			} else {
				Remove(key);
			}
			return this;
		}

		public static String Quote(String s) {
			if (s == null || s.Length == 0) {
				return "\"\"";
			}

			char b;
			char c = '\0';
			int i;
			int len = s.Length;
			StringBuilder sb = new StringBuilder(len + 4);
			String t;

			sb.Append('"');
			for (i = 0; i < len; i += 1) {
				b = c;
				c = s[i];
				switch (c) {
					case '\\':
					case '"':
						sb.Append('\\');
						sb.Append(c);
						break;
					case '/':
						if (b == '<') {
							sb.Append('\\');
						}
						sb.Append(c);
						break;
					case '\b':
						sb.Append("\\b");
						break;
					case '\t':
						sb.Append("\\t");
						break;
					case '\n':
						sb.Append("\\n");
						break;
					case '\f':
						sb.Append("\\f");
						break;
					case '\r':
						sb.Append("\\r");
						break;
					default:
						if (c < ' ' || (c >= '\u0080' && c < '\u00a0') ||
									   (c >= '\u2000' && c < '\u2100')) {
							t = "000" + ((short)c).ToString("X");
							sb.Append("\\u" + t.Substring(t.Length - 4));
						} else {
							sb.Append(c);
						}
						break;
				}
			}
			sb.Append('"');
			return sb.ToString();
		}

		public object Remove(string key) {
			return map.Remove(key);
		}

		internal static object StringToValue(String s) {
			if (s.Equals("")) {
				return s;
			}
			if (String.Compare(s, "true", true) == 0)
				return true;
			if (String.Compare(s, "false", true) == 0)
				return false;
			if (String.Compare(s, "null", true) == 0)
				return Null;

			/*
			 * If it might be a number, try converting it. 
			 * We support the non-standard 0x- convention. 
			 * If a number cannot be produced, then the value will just
			 * be a string. Note that the 0x-, plus, and implied string
			 * conventions are non-standard. A JSON parser may accept
			 * non-JSON forms as long as it accepts all correct JSON forms.
			 */

			char b = s[0];
			if ((b >= '0' && b <= '9') || b == '.' || b == '-' || b == '+') {
				if (b == '0' && s.Length > 2 &&
				    (s[1] == 'x' || s[1] == 'X')) {
					try {
						return Convert.ToInt32(s.Substring(2), 16);
					} catch(Exception) {
					}
				}
				try {
					if (s.IndexOf('.') > -1 ||
					    s.IndexOf('e') > -1 ||
					    s.IndexOf('E') > -1) {
						return Double.Parse(s);
					}

					long value = Int64.Parse(s);
					if (value < Int32.MaxValue)
						return (int) value;
					
					return value;
				} catch(Exception) {
				}
			}
			return s;
		}


		internal static void CheckValid(object o) {
			if (o != null) {
				if (o is double) {
					if (Double.IsInfinity(((Double)o)) ||
						Double.IsNaN((Double)o)) {
						throw new JSONException("JSON does not allow non-finite numbers.");
					}
				} else if (o is float) {
					if (Single.IsInfinity((float)o) ||
						Single.IsNaN((float)o)) {
						throw new JSONException("JSON does not allow non-finite numbers.");
					}
				}
			}
		}

		public JSONArray ToJSONArray(JSONArray names) {
			if (names == null || names.Length == 0) {
				return null;
			}
			JSONArray ja = new JSONArray();
			for (int i = 0; i < names.Length; i += 1) {
				ja.Add(GetValue<object>(names.GetValue(i).ToString()));
			}
			return ja;
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder("{");

			foreach(string key in Keys) {
				if (sb.Length > 1) {
					sb.Append(',');
				}
				sb.Append(Quote(key));
				sb.Append(':');
				sb.Append(ValueToString(map[key]));
			}
			sb.Append('}');
			return sb.ToString();
		}


		public String ToString(int indentFactor) {
			return ToString(indentFactor, 0);
		}


		private string ToString(int indentFactor, int indent) {
			int j;
			int n = Length;
			if (n == 0) {
				return "{}";
			}
			IEnumerator keys = Keys.GetEnumerator();
			StringBuilder sb = new StringBuilder("{");
			int newindent = indent + indentFactor;
			string o;
			if (n == 1) {
				keys.MoveNext();
				o = (string) keys.Current;
				sb.Append(Quote(o));
				sb.Append(": ");
				sb.Append(ValueToString(map[o], indentFactor, indent));
			} else {
				while (keys.MoveNext()) {
					o = (string) keys.Current;
					if (sb.Length > 1) {
						sb.Append(",\n");
					} else {
						sb.Append('\n');
					}
					for (j = 0; j < newindent; j += 1) {
						sb.Append(' ');
					}
					sb.Append(Quote(o));
					sb.Append(": ");
					sb.Append(ValueToString(this.map[o], indentFactor, newindent));
				}
				if (sb.Length > 1) {
					sb.Append('\n');
					for (j = 0; j < indent; j += 1) {
						sb.Append(' ');
					}
				}
			}
			sb.Append('}');
			return sb.ToString();
		}


		internal static String ValueToString(Object value) {
			if (value == null || value.Equals(null))
				return "null";
			if (value is byte || value is short || value is int || value is long ||
				value is float || value is double) {
				return NumberToString(value);
			}
			if (value is bool || value is JSONObject || value is JSONArray) {
				return value.ToString();
			}
			if (value is IDictionary<string, object>) {
				return new JSONObject((IDictionary<string, object>)value).ToString();
			}
			if (value is ICollection) {
				return new JSONArray((ICollection)value).ToString();
			}
			if (value.GetType().IsArray) {
				return new JSONArray(value).ToString();
			}
			return Quote(value.ToString());
		}


		internal static String ValueToString(object value, int indentFactor, int indent) {
			if (value == null || value.Equals(null))
				return "null";

			if (value is byte || value is short || value is int || value is long ||
				value is float || value is double)
				return NumberToString(value);

			if (value is bool)
				return value.ToString();

			if (value is JSONObject)
				return ((JSONObject)value).ToString(indentFactor, indent);
			if (value is JSONArray)
				return ((JSONArray)value).ToString(indentFactor, indent);
			if (value is IDictionary<string, object>)
				return new JSONObject((IDictionary<string, object>)value).ToString(indentFactor, indent);
			if (value is ICollection)
				return new JSONArray((ICollection) value).ToString(indentFactor, indent);
			if (value.GetType().IsArray)
				return new JSONArray(value).ToString(indentFactor, indent);

			return Quote(value.ToString());
		}

		internal static Object Wrap(object obj) {
			try {
				if (obj == null) {
					return Null;
				}
				if (obj is JSONObject || obj is JSONArray ||
						Null.Equals(obj) ||
						obj is byte || obj is char ||
						obj is short || obj is int ||
						obj is long || obj is bool ||
						obj is float || obj is double ||
						obj is String) {
					return obj;
				}

				if (obj is ICollection) {
					return new JSONArray((ICollection)obj);
				}
				if (obj.GetType().IsArray) {
					return new JSONArray(obj);
				}
				if (obj is IDictionary<string, object>) {
					return new JSONObject((IDictionary<string, object>)obj);
				}
				return new JSONObject(obj);
			} catch (Exception) {
				return null;
			}
		}
	}
}