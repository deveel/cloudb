using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	public sealed class ActionAttributes : IEnumerable<KeyValuePair<string, object>>, ICloneable {
		private readonly IAttributesHandler handler;
		private Dictionary<string, object> values;

		internal ActionAttributes(IAttributesHandler handler) {
			this.handler = handler;
			values = new Dictionary<string, object>();
		}

		private void CheckReadOnly() {
			if (handler.IsReadOnly)
				throw new InvalidOperationException("The container is read-only.");
		}

		public object this[string name] {
			get { return values[name]; }
			set {
				CheckReadOnly();
				values[name] = value;
			}
		}

		public int Count {
			get { return values.Count; }
		}

		public ICollection<string> Keys {
			get { return values.Keys; }
		}

		public bool Contains(string name) {
			return values.ContainsKey(name);
		}

		public void Add(string name, object value) {
			CheckReadOnly();
			values.Add(name, value);
		}

		public void Clear() {
			CheckReadOnly();
			values.Clear();
		}

		public bool Remove(string name) {
			CheckReadOnly();
			return values.Remove(name);
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
			return values.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public object Clone() {
			ActionAttributes attributes = new ActionAttributes(handler);
			attributes.values = new Dictionary<string, object>(values.Count);
			foreach(KeyValuePair<string, object> pair in values) {
				object value = pair.Value;
				if (value != null && value is ICloneable)
					value = ((ICloneable) value).Clone();

				attributes.values[pair.Key] = value;
			}
			return attributes;
		}
	}
}