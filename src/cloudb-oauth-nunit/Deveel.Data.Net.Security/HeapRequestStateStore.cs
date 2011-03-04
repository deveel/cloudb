using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class HeapRequestStateStore : IRequestStateStore {
		private readonly object SyncRoot = new object();

		private readonly IDictionary<RequestStateKey, RequestState> states = new Dictionary<RequestStateKey, RequestState>();

		public void Store(RequestState state) {
			if (state == null)
				throw new ArgumentNullException("state");

			lock (SyncRoot) {
				states[state.Key] = state;
			}
		}

		public RequestState Get(RequestStateKey key) {
			if (key == null)
				throw new ArgumentNullException("key");

			lock (SyncRoot) {
				RequestState state;
				if (!states.TryGetValue(key, out state)) {
					state = new RequestState(key);
					Store(state);
				}

				return state;
			}
		}

		public void Delete(RequestStateKey key) {
			if (key == null)
				throw new ArgumentNullException("key");

			lock (SyncRoot) {
				if (states.ContainsKey(key))
					states.Remove(key);
			}
		}
	}
}