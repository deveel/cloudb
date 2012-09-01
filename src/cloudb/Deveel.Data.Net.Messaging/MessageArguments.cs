using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Messaging {
	public sealed class MessageArguments : IList<MessageArgument>, ICloneable {
		internal MessageArguments(bool readOnly) {
			this.readOnly = readOnly;
			children = new List<MessageArgument>();
		}

		private bool readOnly;
		private List<MessageArgument> children;
		private string[] keys;

		private int unnamedCount = -1;

		private const string UnnamedKey = "#ARG{0}#";
		
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

		private void CheckHasChild(MessageArgument arg) {
			if (Contains(arg))
				throw new ArgumentException("This argument was already inserted.");
		}

		internal void Seal() {
			readOnly = true;

			for (int i = 0; i < children.Count; i++) {
				children[i].Seal();
			}
		}

		public IEnumerator<MessageArgument> GetEnumerator() {
			return children.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		internal void SafeAdd(MessageArgument item) {
			CheckHasChild(item);
			children.Add(item);
		}

		public void Add(MessageArgument item) {
			CheckReadOnly();
			SafeAdd(item);
		}

		public MessageArgument Add(string name, object value) {
			MessageArgument item = new MessageArgument(name, value);
			Add(item);
			return item;
		}

		public MessageArgument Add(object value) {
			return Add(String.Format(UnnamedKey, ++unnamedCount), value);
		}

		public void Clear() {
			CheckReadOnly();
			children.Clear();
		}

		public bool Contains(MessageArgument item) {
			return IndexOf(item) != -1;
		}

		public bool Contains(string name) {
			return IndexOf(name) != -1;
		}

		public void CopyTo(MessageArgument[] array, int arrayIndex) {
			children.CopyTo(array, arrayIndex);
		}

		public bool Remove(MessageArgument item) {
			CheckReadOnly();
			return children.Remove(item);
		}

		public bool Remove(string name) {
			CheckReadOnly();

			int removeCount = 0;
			for(int i = children.Count - 1; i > 0; i--) {
				MessageArgument arg = children[i];
				if (arg.Name.Equals(name)) {
					children.RemoveAt(i);
					removeCount++;
				}
			}
						
			return removeCount > 0;
		}
		
		public MessageArgument RemoveFirst(string name) {
			CheckReadOnly();
			
			int index = IndexOf(name);
			if (index == -1)
				return null;
			
			MessageArgument arg = children[index];
			children.RemoveAt(index);
			return arg;
		}

		public int Count {
			get { return children.Count; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		public int IndexOf(MessageArgument item) {
			for (int i = 0; i < children.Count; i++) {
				MessageArgument child = children[i];
				if (child.Equals(item))
					return i;
			}

			return -1;
		}

		public int IndexOf(string name) {
			for (int i = 0; i < children.Count; i++) {
				MessageArgument child = children[i];
				if (child.Name.Equals(name))
					return i;
			}

			return -1;
		}

		public void Insert(int index, MessageArgument item) {
			CheckReadOnly();
			CheckHasChild(item);
			children.Insert(index, item);
		}

		public void Insert(int index, object value) {
			Insert(index, new MessageArgument(String.Format(UnnamedKey, ++unnamedCount)));
		}

		public void RemoveAt(int index) {
			CheckReadOnly();
			children.RemoveAt(index);
		}

		public MessageArgument this[int index] {
			get { return children[index]; }
			set {
				CheckReadOnly();
				children[index] = value;
			}
		}

		public MessageArgument this[string name] {
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
		
		public MessageArgument[] GetArguments(string name) {
			List<MessageArgument> args = new List<MessageArgument>(Count);
			for(int i = 0; i < children.Count; i++) {
				MessageArgument arg = children[i];
				if (arg.Name.Equals(name))
					args.Add(arg);
			}
			
			return args.ToArray();
		}

		public object Clone() {
			MessageArguments c = new MessageArguments(readOnly);
			c.children = new List<MessageArgument>(children.Count);
			for (int i = 0; i < children.Count; i++) {
				c.children.Add((MessageArgument)children[i].Clone());
			}
			return c;
		}
	}
}