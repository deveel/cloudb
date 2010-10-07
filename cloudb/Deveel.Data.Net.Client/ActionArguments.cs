using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Client {
	public sealed class ActionArguments : IList<ActionArgument>, ICloneable {
		internal ActionArguments(bool readOnly) {
			this.readOnly = readOnly;
			children = new List<ActionArgument>();
		}

		private bool readOnly;
		private List<ActionArgument> children;
		private string[] keys;
		
		public string[] Names {
			get {
				if (keys == null) {
					List<string> c = new List<string>(Count);
					for (int i = 0; i < children.Count; i++) {
						string name = children[i].Name;
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

		private void CheckHasChild(ActionArgument arg) {
			if (Contains(arg))
				throw new ArgumentException("This argument was already inserted.");
		}

		internal void Seal() {
			readOnly = true;

			for (int i = 0; i < children.Count; i++) {
				children[i].Seal();
			}
		}

		public IEnumerator<ActionArgument> GetEnumerator() {
			return children.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		internal void SafeAdd(ActionArgument item) {
			CheckHasChild(item);
			children.Add(item);
		}

		public void Add(ActionArgument item) {
			CheckReadOnly();
			SafeAdd(item);
		}

		public ActionArgument Add(string name, object value) {
			ActionArgument item = new ActionArgument(name, value);
			Add(item);
			return item;
		}

		public void Clear() {
			CheckReadOnly();
			children.Clear();
		}

		public bool Contains(ActionArgument item) {
			return IndexOf(item) != -1;
		}

		public bool Contains(string name) {
			return IndexOf(name) != -1;
		}

		public void CopyTo(ActionArgument[] array, int arrayIndex) {
			children.CopyTo(array, arrayIndex);
		}

		public bool Remove(ActionArgument item) {
			CheckReadOnly();
			return children.Remove(item);
		}

		public bool Remove(string name) {
			CheckReadOnly();

			int removeCount = 0;
			for(int i = children.Count - 1; i > 0; i--) {
				ActionArgument arg = children[i];
				if (arg.Name.Equals(name)) {
					children.RemoveAt(i);
					removeCount++;
				}
			}
						
			return removeCount > 0;
		}
		
		public ActionArgument RemoveFirst(string name) {
			CheckReadOnly();
			
			int index = IndexOf(name);
			if (index == -1)
				return null;
			
			ActionArgument arg = children[index];
			children.RemoveAt(index);
			return arg;
		}

		public int Count {
			get { return children.Count; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		public int IndexOf(ActionArgument item) {
			for (int i = 0; i < children.Count; i++) {
				ActionArgument child = children[i];
				if (child.Equals(item))
					return i;
			}

			return -1;
		}

		public int IndexOf(string name) {
			for (int i = 0; i < children.Count; i++) {
				ActionArgument child = children[i];
				if (child.Name.Equals(name))
					return i;
			}

			return -1;
		}

		public void Insert(int index, ActionArgument item) {
			CheckReadOnly();
			CheckHasChild(item);
			children.Insert(index, item);
		}

		public void RemoveAt(int index) {
			CheckReadOnly();
			children.RemoveAt(index);
		}

		public ActionArgument this[int index] {
			get { return children[index]; }
			set {
				CheckReadOnly();
				children[index] = value;
			}
		}

		public ActionArgument this[string name] {
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
		
		public ActionArgument[] GetArguments(string name) {
			List<ActionArgument> args = new List<ActionArgument>(Count);
			for(int i = 0; i < children.Count; i++) {
				ActionArgument arg = children[i];
				if (arg.Name.Equals(name))
					args.Add(arg);
			}
			
			return args.ToArray();
		}

		public object Clone() {
			ActionArguments c = new ActionArguments(readOnly);
			c.children = new List<ActionArgument>(children.Count);
			for (int i = 0; i < children.Count; i++) {
				c.children.Add((ActionArgument)children[i].Clone());
			}
			return c;
		}
	}
}