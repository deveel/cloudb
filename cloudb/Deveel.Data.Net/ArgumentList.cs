using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class ArgumentList : IList<MethodArgument>, ICloneable {
		internal ArgumentList(bool readOnly) {
			this.readOnly = readOnly;
			children = new List<MethodArgument>();
		}

		private bool readOnly;
		private List<MethodArgument> children;

		private void CheckReadOnly() {
			if (readOnly)
				throw new InvalidOperationException("The list is read-only.");
		}

		private void CheckHasChild(MethodArgument arg) {
			if (Contains(arg))
				throw new ArgumentException("This argument was already inserted.");
		}

		internal void Seal() {
			readOnly = true;
		}

		public IEnumerator<MethodArgument> GetEnumerator() {
			return children.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		internal void SafeAdd(MethodArgument item) {
			CheckHasChild(item);
			children.Add(item);
		}

		public void Add(MethodArgument item) {
			CheckReadOnly();
			SafeAdd(item);
		}

		public void Clear() {
			CheckReadOnly();
			children.Clear();
		}

		public bool Contains(MethodArgument item) {
			return IndexOf(item) != -1;
		}

		public void CopyTo(MethodArgument[] array, int arrayIndex) {
			children.CopyTo(array, arrayIndex);
		}

		public bool Remove(MethodArgument item) {
			CheckReadOnly();
			return children.Remove(item);
		}

		public int Count {
			get { return children.Count; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		public int IndexOf(MethodArgument item) {
			for (int i = 0; i < children.Count; i++) {
				MethodArgument child = children[i];
				if (child.Equals(item))
					return i;
			}

			return -1;
		}

		public void Insert(int index, MethodArgument item) {
			CheckReadOnly();
			CheckHasChild(item);
			children.Insert(index, item);
		}

		public void RemoveAt(int index) {
			CheckReadOnly();
			children.RemoveAt(index);
		}

		public MethodArgument this[int index] {
			get { return children[index]; }
			set {
				CheckReadOnly();
				children[index] = value;
			}
		}

		public object Clone() {
			ArgumentList c = new ArgumentList(readOnly);
			c.children = new List<MethodArgument>(children.Count);
			for (int i = 0; i < children.Count; i++) {
				c.children.Add((MethodArgument)children[i].Clone());
			}
			return c;
		}
	}
}