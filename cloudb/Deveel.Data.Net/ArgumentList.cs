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
		private string[] keys;
		
		public string[] Names {
			get {
				if (keys == null) {
					List<string> c = new List<string>(Count);
					for (int i = 0; i < children.Count; i++) {
						string name = children[i];
						if (!c.Contains(name))
							c.Add(name);
					}
					
					keys = c.ToArray();
				}
				
				return keys;
			}
		}

		private void CheckReadOnly() {
			if (readOnly)
				throw new InvalidOperationException("The list is read-only.");
			
			keys = null;
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

		public MethodArgument Add(string name, object value) {
			MethodArgument item = new MethodArgument(name, value);
			Add(item);
			return item;
		}

		public void Clear() {
			CheckReadOnly();
			children.Clear();
		}

		public bool Contains(MethodArgument item) {
			return IndexOf(item) != -1;
		}

		public bool Contains(string name) {
			return IndexOf(name) != -1;
		}

		public void CopyTo(MethodArgument[] array, int arrayIndex) {
			children.CopyTo(array, arrayIndex);
		}

		public bool Remove(MethodArgument item) {
			CheckReadOnly();
			return children.Remove(item);
		}

		public bool Remove(string name) {
			CheckReadOnly();

			int removeCount = 0;
			for(int i = children.Count - 1; i > 0; i--) {
				MethodArgument arg = children[i];
				if (arg.Name.Equals(name)) {
					children.RemoveAt(i);
					removeCount++;
				}
			}
						
			return removeCount > 0;
		}
		
		public MethodArgument RemoveFirst(string name) {
			CheckReadOnly();
			
			int index = IndexOf(name);
			if (index == -1)
				return null;
			
			MethodArgument arg = children[index];
			children.RemoveAt(index);
			return arg;
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

		public int IndexOf(string name) {
			for (int i = 0; i < children.Count; i++) {
				MethodArgument child = children[i];
				if (child.Name.Equals(name))
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

		public MethodArgument this[string name] {
			get { 
				int index = IndexOf(name);
				return index == -1 ? null : children[index];
			}
			set {
				CheckReadOnly();
				int index = IndexOf(name);
				if (index == -1) {
					Add(value);
				} else {
					children[index] = value;
				}
			}
		}
		
		public MethodArgument[] GetArguments(string name) {
			List<MethodArgument> args = new List<MethodArgument>(Count);
			for(int i = 0; i < children.Count; i++) {
				MethodArgument arg = children[i];
				if (arg.Name.Equals(name))
					args.Add(arg);
			}
			
			return args.ToArray();
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