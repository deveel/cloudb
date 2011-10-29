using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public sealed class ConnectionCollection : ICollection<IConnection> {
		private readonly ServiceConnector connector;
		private readonly List<IConnection> connections;

		internal ConnectionCollection (ServiceConnector connector) {
			this.connector = connector;
			connections = new List<IConnection>();
		}

		public IEnumerator<IConnection> GetEnumerator() {
			return connections.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Add(IConnection item) {
			if (Contains(item))
				throw new ArgumentException("Connection to '" + item.Address + "' already established.");

			connections.Add(item);
			connector.OnConnectionAdded(item);
		}

		public void Clear() {
			for (int i = connections.Count - 1; i >= 0; i--) {
				IConnection connection = connections[i];
				connector.OnConnectionRemoved(connection);
				connections.RemoveAt(i);
			}
		}

		public bool Contains(IServiceAddress address) {
			for (int i = connections.Count - 1; i >= 0; i--) {
				if (connections[i].Address.Equals(address))
					return true;
			}

			return false;
		}

		public bool Contains(IConnection item) {
			if (item == null)
				throw new ArgumentNullException("item");

			return Contains(item.Address);
		}

		public void CopyTo(IConnection[] array, int arrayIndex) {
			connections.CopyTo(array, arrayIndex);
		}

		public bool Remove(IConnection item) {
			if (item == null)
				throw new ArgumentNullException("item");

			return Remove(item.Address);
		}

		public bool Remove(IServiceAddress address) {
			for (int i = connections.Count - 1; i >= 0; i--) {
				IConnection connection = connections[i];
				if (connection.Address.Equals(address)) {
					connections.RemoveAt(i);
					connector.OnConnectionRemoved(connection);
					return true;
				}
			}

			return false;
		}

		public int Count {
			get { return connections.Count; }
		}

		bool ICollection<IConnection>.IsReadOnly {
			get { return false; }
		}

		public bool TryGetConnection(IServiceAddress address, out IConnection connection) {
			for (int i = connections.Count - 1; i >= 0; i--) {
				IConnection c = connections[i];
				if (c.Address.Equals(address)) {
					connection = c;
					return true;
				}
			}

			connection = null;
			return false;
		}
	}
}

