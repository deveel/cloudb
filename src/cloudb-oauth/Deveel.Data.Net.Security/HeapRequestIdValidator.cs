using System;
using System.Collections.Generic;
using System.Threading;

namespace Deveel.Data.Net.Security {
	public sealed class HeapRequestIdValidator : IRequestIdValidator, IDisposable {
		private readonly Timer purgeTimer;
		private readonly IDictionary<long, IList<RequestId>> requestCache;
		private readonly long halfWindow;

		private static readonly object SyncRoot = new object();

		public HeapRequestIdValidator(long timeWindow) {
			requestCache = new Dictionary<long, IList<RequestId>>();
			halfWindow = timeWindow/2;

			TimeSpan span = new TimeSpan((int)(timeWindow * 1.5) * TimeSpan.TicksPerSecond);
			purgeTimer = new Timer(Purge, null, span, span);
		}

		private long ValidateTimestamp(long timestamp, long now) {
			// Parse the timestamp (it must be a positive integer) and check 
			// the timestamp is within +/ HalfWindow from the current time  
			if (timestamp <= 0 || (now - timestamp) > halfWindow)
				throw new TimestampRefusedException(null, now - halfWindow, now + halfWindow);
				
			return timestamp;
		}

		private RequestId ValidateNonce(string nonce, long timestamp, string consumerKey, string token) {
			RequestId currentId = new RequestId(timestamp, nonce, consumerKey, token);

			bool foundClash = false;

			// Lock the request cache while we look for the current id
			lock (SyncRoot) {
				IList<RequestId> requests;
				if (requestCache.TryGetValue(currentId.Timestamp, out requests)) {
					foreach (RequestId request in requests)
						if (request == currentId) {
							foundClash = true;
							break;
						}
				}

				// If we didn't find a clash, store the current id in the cache
				if (!foundClash) {
					if (!requestCache.TryGetValue(currentId.Timestamp, out requests)) {
						requests = new List<RequestId>();
						requestCache.Add(currentId.Timestamp, requests);
					}

					requests.Add(currentId);
				}
			}

			// If we did find a clash, throw a nonce used OAuthRequestException
			if (foundClash)
				throw new OAuthRequestException(null, OAuthProblemTypes.NonceUsed);

			return currentId;
		}



		private void Purge(object state) {
			long now = UnixTime.ToUnixTime(DateTime.Now);

			long[] timestamps;

			// Lock to get keys
			lock (SyncRoot) {
				timestamps = new long[requestCache.Count];
				requestCache.Keys.CopyTo(timestamps, 0);
			}

			foreach (long timestamp in timestamps)
				if ((timestamp + halfWindow) < now)
					requestCache.Remove(timestamp);

		}

		public RequestId ValidateRequest(string nonce, long timestamp, string consumerKey, string requestToken) {
			// Compute the server time
			long now = UnixTime.ToUnixTime(DateTime.Now);

			// Get and validate the timestamp
			long ts = ValidateTimestamp(timestamp, now);

			return ValidateNonce(nonce, ts, consumerKey, requestToken);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (purgeTimer != null) {
					purgeTimer.Dispose();
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}