using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthMessageArguments : ICollection<AuthMessageArgument> {
		private readonly List<AuthMessageArgument> list;
		private readonly AuthMessage message;

		internal AuthMessageArguments(AuthMessage message) {
			this.message = message;
			list = new List<AuthMessageArgument>();
		}

		private void AssertMessageIsNotReadOnly() {
			if (message.IsReadOnly)
				throw new InvalidOperationException("The message is read-only.");
		}

		public IEnumerator<AuthMessageArgument> GetEnumerator() {
			return list.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public AuthMessageArgument this[string argName] {
			get { 
				int index = IndexOf(argName);
				return (index == -1 ? null : list[index]);
			}
		}

		public void Add(AuthMessageArgument item) {
			AssertMessageIsNotReadOnly();

			if (Contains(item.Name))
				throw new ArgumentException("The argument '" + item.Name + "' is already present.");

			list.Add(item);
		}

		public AuthMessageArgument Add(string argName, AuthObject argValue) {
			AssertMessageIsNotReadOnly();

			if (Contains(argName))
				throw new ArgumentException("The argument '" + argName + "' is already present.");

			AuthMessageArgument arg = new AuthMessageArgument(argName, argValue);
			list.Add(arg);
			return arg;
		}

		public AuthMessageArgument Add(string argName, object argValue) {
			return Add(argName, new AuthObject(argValue));
		}

		public void Clear() {
			AssertMessageIsNotReadOnly();
			list.Clear();
		}

		public bool Contains(AuthMessageArgument item) {
			return Contains(item.Name);
		}

		public bool Contains(string argName) {
			return IndexOf(argName) != -1;
		}

		public int IndexOf(string argName) {
			for (int i = list.Count - 1; i >= 0; i--) {
				if (String.Equals(argName, list[i].Name, StringComparison.InvariantCultureIgnoreCase))
					return i;
			}

			return -1;
		}

		public void CopyTo(AuthMessageArgument[] array, int arrayIndex) {
			list.CopyTo(array, arrayIndex);
		}

		public bool Remove(AuthMessageArgument item) {
			return Remove(item.Name);
		}

		public bool Remove(string argName) {
			AssertMessageIsNotReadOnly();

			int index = IndexOf(argName);
			if (index == -1)
				return false;

			list.RemoveAt(index);
			return true;
		}

		public int Count {
			get { return list.Count; }
		}

		public bool IsReadOnly {
			get { return message.IsReadOnly; }
		}
	}
}