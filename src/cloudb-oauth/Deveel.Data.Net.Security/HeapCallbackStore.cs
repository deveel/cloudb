using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class HeapCallbackStore : ICallbackStore {
		private readonly Dictionary<IRequestToken,Uri> heapStore = new Dictionary<IRequestToken, Uri>();

		public bool SaveCallback(IRequestToken token, Uri callback) {
			lock (heapStore) {
				bool found = heapStore.ContainsKey(token);
				if (!found)
					heapStore.Add(token, callback);

				return found;
			}
		}

		public bool ObtainCallback(IRequestToken token, out Uri callback) {
			lock (heapStore) {
				if (heapStore.TryGetValue(token, out callback)) {
					heapStore.Remove(token);
					return true;
				}

				return false;
			}
		}
	}
}