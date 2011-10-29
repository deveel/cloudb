using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class AuthenticatorCollection : ICollection<IServiceAuthenticator> {
		private readonly List<IServiceAuthenticator> authenticators;

		internal AuthenticatorCollection() {
			authenticators = new List<IServiceAuthenticator>();
		}

		public IServiceAuthenticator this[string mechanism] {
			get { 
				IServiceAuthenticator authenticator;
				return TryGetAuthenticator(mechanism, out authenticator) ? authenticator : null;
			}
		}

		public bool TryGetAuthenticator(string mechanism, out IServiceAuthenticator authenticator) {
			if (String.IsNullOrEmpty(mechanism))
				throw new ArgumentNullException("mechanism");

			for (int i = authenticators.Count - 1; i >= 0; i--) {
				IServiceAuthenticator a = authenticators[i];
				if (String.Equals(a.Mechanism, mechanism, StringComparison.InvariantCultureIgnoreCase)) {
					authenticator = a;
					return true;
				}
			}

			authenticator = null;
			return false;
		}

		public IEnumerator<IServiceAuthenticator> GetEnumerator() {
			return authenticators.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Add(IServiceAuthenticator item) {
			if (item == null)
				throw new ArgumentNullException("item");
			if (String.IsNullOrEmpty(item.Mechanism))
				throw new ArgumentException("The authenticator does not specify any mechanism name.");

			if (Contains(item.Mechanism))
				throw new ArgumentException("An authenticator for the mechanism '" + item.Mechanism + "' was already defined.");

			authenticators.Add(item);
		}

		public void Clear() {
			authenticators.Clear();
		}

		public bool Contains(string mechanism) {
			for (int i = authenticators.Count - 1; i >= 0; i--) {
				if (String.Equals(mechanism, authenticators[i].Mechanism, StringComparison.InvariantCultureIgnoreCase))
					return true;
			}

			return false;
		}

		public bool Contains(IServiceAuthenticator item) {
			return Contains(item.Mechanism);
		}

		public void CopyTo(IServiceAuthenticator[] array, int arrayIndex) {
			authenticators.CopyTo(array, arrayIndex);
		}

		public bool Remove(IServiceAuthenticator item) {
			return Remove(item.Mechanism);
		}

		public bool Remove(string mechanism) {
			for (int i = authenticators.Count - 1; i >= 0; i--) {
				if (String.Equals(mechanism, authenticators[i].Mechanism, StringComparison.InvariantCultureIgnoreCase)) {
					authenticators.RemoveAt(i);
					return true;
				}
			}

			return false;
		}

		public int Count {
			get { return authenticators.Count; }
		}

		bool ICollection<IServiceAuthenticator>.IsReadOnly {
			get { return false; }
		}
	}
}