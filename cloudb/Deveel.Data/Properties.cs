using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class Properties {
		private readonly StringCollection collection;

		private static readonly PropertyComparer DefaultPropertyComparer = new PropertyComparer();
		private static readonly KeyComparer DefaultKeyComparer = new KeyComparer();

		public Properties(DataFile file) {
			collection = new StringCollection(file, DefaultPropertyComparer);
		}

		public ISortedCollection<string> Keys {
			get { return new KeysCollection(collection); }
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

		public void SetProperty(string key, string value) {
			CheckValidKey(key);

			// If the value is being removed, remove the key from the set,
			if (value == null) {
				// If the key isn't found in 'strings_set', then nothing changes,
				collection.Remove(key);
			} else {
				// Get the current value for this key in the set
				String cur_val = GetProperty(key);
				// If there's a value currently stored,
				if (cur_val != null) {
					// If we are setting the key to the same value, we exit the function
					// early because nothing needs to be done,
					if (cur_val.Equals(value))
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

		public string GetProperty(string key) {
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

		public string GetProperty(string key, string defaultValue) {
			string str = GetProperty(key);
			if (str == null)
				return defaultValue;
			return str;
		}

		private sealed class KeysCollection : ISortedCollection<string> {
			private readonly StringCollection collection;

			public KeysCollection(StringCollection collection) {
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

			public void Add(string item) {
				throw new NotSupportedException();
			}

			public void Clear() {
				collection.Clear();
			}

			public bool Contains(string item) {
				return collection.Contains(item);
			}

			public void CopyTo(string[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(string item) {
				return collection.Remove(item);
			}

			public int Count {
				get { return collection.Count; }
			}

			public bool IsReadOnly {
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