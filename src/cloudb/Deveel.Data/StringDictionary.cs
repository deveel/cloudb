//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data {
	public sealed class StringDictionary {
		private readonly StringCollection collection;

		private static readonly PropertyComparer DefaultPropertyComparer = new PropertyComparer();
		private static readonly KeyComparer DefaultKeyComparer = new KeyComparer();

		public StringDictionary(IDataFile file) {
			collection = new StringCollection(file, DefaultPropertyComparer);
		}

		public KeysCollection Keys {
			get { return new KeysCollection(collection); }
		}

		public int Count {
			get { return collection.Count; }
		}

		public string this[string name] {
			get { return GetValue(name); }
			set { SetValue(name, value); }
		}

		private static String KeyValuePart(string value) {
			int delim = value.IndexOf('=');
			return delim == -1 ? value : value.Substring(0, delim);
		}

		private static void CheckValidKey(string key) {
			int sz = key.Length;
			for (int i = 0; i < sz; ++i) {
				char c = key[i];
				if (c == '=') {
					throw new ApplicationException("Invalid character in key.");
				}
			}
		}

		public void SetValue(string key, string value) {
			CheckValidKey(key);

			// If the value is being removed, remove the key from the set,
			if (value == null) {
				// If the key isn't found in 'strings_set', then nothing changes,
				collection.Remove(key);
			} else {
				// Get the current value for this key in the set
				string curVal = GetValue(key);
				// If there's a value currently stored,
				if (curVal != null) {
					// If we are setting the key to the same value, we exit the function
					// early because nothing needs to be done,
					if (curVal.Equals(value))
						return;

					// Otherwise remove the existing key
					collection.Remove(key);
				}

				// Add the key
				StringBuilder sb = new StringBuilder(key.Length + value.Length + 1);
				sb.Append(key);
				sb.Append('=');
				sb.Append(value);
				collection.Add(sb.ToString());
			}
		}

		public void SetValue<T>(string  key, T value) where  T : IConvertible {
			if (value == null || value.Equals(default(T))) {
				SetValue(key,  null);
			} else {
				SetValue(key, Convert.ToString(value));
			}
		}

		public string GetValue(string key) {
			CheckValidKey(key);
			StringCollection s1 = collection.Tail(key);
			if (!s1.IsEmpty) {
				string entry = s1.First;
				int delim = entry.IndexOf('=');
				if (entry.Substring(0, delim).Equals(key)) {
					// Found the key, so return the value
					return entry.Substring(delim + 1);
				}
			}
			return null;
		}

		public string GetValue(string key, string defaultValue) {
			string str = GetValue(key);
			if (str == null)
				return defaultValue;
			return str;
		}

		public T GetValue<T>(string key, T defaultValue) where T: IConvertible {
			string s = GetValue(key);
			if (String.IsNullOrEmpty(s))
				return defaultValue;
			return (T) Convert.ChangeType(s, typeof(T));
		}

		public T GetValue<T>(string key) where T : IConvertible {
			return GetValue(key, default(T));
		}

		public sealed class KeysCollection : ISortedCollection<string> {
			private readonly StringCollection collection;

			internal KeysCollection(StringCollection collection) {
				this.collection = collection;
			}

			#region Implementation of IEnumerable

			public IEnumerator<string> GetEnumerator() {
				return new Enumerator((IInteractiveEnumerator<string>) collection.GetEnumerator());
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			#endregion

			#region Implementation of ICollection<string>

			void ICollection<string>.Add(string item) {
				throw new NotSupportedException();
			}

			public void Clear() {
				collection.Clear();
			}

			public bool Contains(string item) {
				return collection.Contains(item);
			}

			void ICollection<string>.CopyTo(string[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(string item) {
				return collection.Remove(item);
			}

			public int Count {
				get { return collection.Count; }
			}

			bool ICollection<string>.IsReadOnly {
				get { return false; }
			}

			#endregion

			#region Implementation of ISortedCollection<string>

			public IComparer<string> Comparer {
				get { return DefaultKeyComparer; }
			}

			public string First {
				get { return KeyValuePart(collection.First); }
			}

			public string Last {
				get { return KeyValuePart(collection.Last); }
			}

			public ISortedCollection<string> Tail(string start) {
				return new KeysCollection(collection.Tail(start));
			}

			public ISortedCollection<string> Head(string end) {
				return new KeysCollection(collection.Head(end));
			}

			public ISortedCollection<string> Sub(string start, string end) {
				return new KeysCollection(collection.Sub(start, end));
			}

			#endregion

			private class Enumerator : IInteractiveEnumerator<string> {
				private readonly IInteractiveEnumerator<string> enumerator;

				public Enumerator(IInteractiveEnumerator<string> enumerator) {
					this.enumerator = enumerator;
				}

				#region Implementation of IDisposable

				public void Dispose() {
				}

				#endregion

				public void Remove() {
					enumerator.Remove();
				}

				#region Implementation of IEnumerator

				public bool MoveNext() {
					return enumerator.MoveNext();
				}

				public void Reset() {
					enumerator.Reset();
				}

				public string Current {
					get { return KeyValuePart(enumerator.Current); }
				}

				object IEnumerator.Current {
					get { return Current; }
				}

				#endregion
			}
		}

		private class PropertyComparer : IComparer<string> {
			#region Implementation of IComparer<string>

			public int Compare(string x, string y) {
				// Compare the keys of the string
				return KeyValuePart(x).CompareTo(KeyValuePart(y));
			}

			#endregion
		}

		private sealed class KeyComparer : IComparer<string> {
			#region Implementation of IComparer<string>

			public int Compare(string x, string y) {
				return x.CompareTo(y);
			}

			#endregion
		}
	}
}